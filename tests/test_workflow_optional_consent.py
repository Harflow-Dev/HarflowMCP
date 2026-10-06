"""Optional reviewer bypass is a structured opt-in, never natural-language consent."""
import copy
import json

import pytest

from polybridge import workflow_delegation as d, workflow_responses as responses, workflows as w
from test_workflow_delegation import storage  # noqa: F401
from test_workflow_optional_refusal import ANSWER, suspended


@pytest.mark.parametrize('answer', ['No, do not proceed without Vibe', 'Proceed without Vibe'])
async def test_text_alone_does_not_authorize_skip(storage, tmp_path, answer):
    run, _ = await suspended(storage, tmp_path)
    assert run['optional_review_skip_available'] is True
    assert responses.compact(run)['optional_review_skip_available'] is True
    assert responses.monitor(run)['optional_review_skip_available'] is True
    resumed = storage.control(run['workflow_run_id'], 'resume', instructions=answer, decision_id=run['input_decision_id'])
    assert not resumed.get('optional_skip_authorizations')
    assert 'optional_review_skip_available' not in resumed
    token = next(t for t in resumed['pending'] if t['node_id'] == 'right')
    node = next(n for n in resumed['definition']['nodes'] if n['id'] == 'right')
    assert not any(c['kind'] == 'skip_optional_review' for c in d.continuations(resumed, node, token, False, root=storage.root))


@pytest.mark.parametrize('invalid', [None, 0, 1, 'true', [], {}])
async def test_consent_requires_strict_boolean_before_mutation(storage, tmp_path, invalid):
    run, _ = await suspended(storage, tmp_path)
    with pytest.raises(w.WorkflowError, match='must be a boolean'):
        storage.control(run['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=run['input_decision_id'], allow_optional_review_skip=invalid)
    assert storage.get_run(run['workflow_run_id']) == run


@pytest.mark.parametrize('invalid', ['required', 'not_refused', 'attention', 'recover', 'pause', 'cancel', 'stale'])
async def test_explicit_consent_rejects_ineligible_scope_before_mutation(storage, tmp_path, invalid):
    run, _ = await suspended(storage, tmp_path)
    def change(r):
        if invalid == 'required': next(n for n in r['definition']['nodes'] if n['id'] == 'right')['optional'] = False
        if invalid == 'not_refused': next(a for a in r['activations'] if a['role'] == 'node' and a['node_id'] == 'right')['tasks'][0]['result']['permission_denials'] = []
        if invalid == 'attention': r['status'] = 'needs_attention'
    before = storage.update_run(run['workflow_run_id'], change, 'fixture')
    with pytest.raises(w.WorkflowError):
        storage.control(run['workflow_run_id'], invalid if invalid in {'recover', 'pause', 'cancel'} else 'resume', instructions=ANSWER, decision_id='stale' if invalid == 'stale' else run['input_decision_id'], allow_optional_review_skip=True)
    assert storage.get_run(run['workflow_run_id']) == before


async def forwarded(storage, tmp_path):
    """A suspended source and a lightweight caller root exercise delivery authority."""
    child, _ = await suspended(storage, tmp_path)
    root = copy.deepcopy(child)
    root_id = 'forwarding-root'
    root.update(workflow_run_id=root_id, pending=[], activations=[], joins={}, input_source={'workflow_run_id': child['workflow_run_id']}, optional_review_skip_available=True)
    (storage.runs / (root_id + '.json')).write_text(json.dumps(root))
    return root, child


@pytest.mark.parametrize('failure', ['before_child_write', 'after_child_write', 'parent_write'])
async def test_forwarded_consent_delivery_is_exact_and_idempotent(storage, tmp_path, monkeypatch, failure):
    root, child = await forwarded(storage, tmp_path)
    original = storage.update_run
    failed = False
    def injected(identifier, change, event, detail=None):
        nonlocal failed
        target = identifier == child['workflow_run_id'] and event == 'control:resume' if failure != 'parent_write' else identifier == root['workflow_run_id'] and event == 'forwarded_input_answered'
        if target and not failed:
            failed = True
            if failure == 'after_child_write': original(identifier, change, event, detail)
            raise OSError('injected failure')
        return original(identifier, change, event, detail)
    monkeypatch.setattr(storage, 'update_run', injected)
    options = dict(instructions=ANSWER, decision_id=child['input_decision_id'], allow_optional_review_skip=True)
    with pytest.raises(OSError): storage.control(root['workflow_run_id'], 'resume', **options)
    pending = storage.get_run(root['workflow_run_id'])['forwarded_delivery']
    assert pending['payload']['allow_optional_review_skip'] is True
    with pytest.raises(w.WorkflowError): storage.control(root['workflow_run_id'], 'resume', **(options | {'allow_optional_review_skip': False}))
    result = storage.control(root['workflow_run_id'], 'resume', **options)
    assert result['status'] == 'running' and 'optional_review_skip_available' not in result
    grants = storage.get_run(child['workflow_run_id'])['optional_skip_authorizations']
    assert len(grants) == 1 and next(iter(grants.values()))['allow_optional_review_skip'] is True
    assert storage.get_run(child['workflow_run_id'])['forwarded_delivery_receipt'] == pending


async def test_forwarded_ineligible_opt_in_does_not_prepare_delivery(storage, tmp_path):
    root, child = await forwarded(storage, tmp_path)
    storage.update_run(child['workflow_run_id'], lambda r: r.update(status='needs_attention'), 'fixture')
    with pytest.raises(w.WorkflowError):
        storage.control(root['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=child['input_decision_id'], allow_optional_review_skip=True)
    assert storage.get_run(root['workflow_run_id']) == root


@pytest.mark.parametrize('allow', [False, True])
@pytest.mark.parametrize('monitor', [False, True])
def test_cli_passes_explicit_consent_only_when_flag_is_present(monkeypatch, capsys, allow, monitor):
    from polybridge import ctl, server
    calls = []
    async def call(action, **kwargs):
        calls.append((action, kwargs))
        return {'workflow_run_id': 'run', 'status': 'running'}
    monkeypatch.setattr(server, '_workflow_call', call)
    argv = ['workflow-resume', 'run', '--instructions', ANSWER, '--decision-id', 'current', '--json']
    if allow: argv.append('--allow-optional-review-skip')
    if monitor: argv.append('--monitor')
    assert ctl.main(argv) == 0
    assert calls[0][0] == 'resume'
    assert calls[0][1]['allow_optional_review_skip'] is allow
    assert calls[0][1]['decision_id'] == 'current'
    if monitor: assert calls[0][1]['interaction_owner'] == 'monitor'
    capsys.readouterr()


@pytest.mark.parametrize('allow', [False, True])
async def test_mcp_resume_passes_structured_consent(monkeypatch, allow):
    from polybridge import server
    calls = []
    async def call(action, **kwargs):
        calls.append(kwargs)
        return {'workflow_run_id': 'run', 'status': 'running'}
    monkeypatch.setattr(server, '_workflow_call', call)
    await server.resume_workflow('run', ANSWER, decision_id='current', allow_optional_review_skip=allow)
    assert calls[0]['allow_optional_review_skip'] is allow


@pytest.mark.parametrize('invalid', [None, 0, 1, 'true', 'false'])
async def test_mcp_binding_does_not_coerce_consent(monkeypatch, invalid):
    from polybridge import server
    from test_server import call
    async def unexpected(*args, **kwargs):
        pytest.fail('Invalid consent reached workflow dispatch')
    monkeypatch.setattr(server, '_workflow_call', unexpected)
    result = await call('resume_workflow', workflow_run_id='run', instructions=ANSWER, decision_id='current', allow_optional_review_skip=invalid)
    assert result.is_error


@pytest.mark.parametrize('allow', [False, True])
async def test_mcp_binding_preserves_real_consent_boolean(monkeypatch, allow):
    from polybridge import server
    from test_server import call
    calls = []
    async def dispatch(action, **kwargs):
        calls.append(kwargs)
        return {'workflow_run_id': 'run', 'status': 'running'}
    monkeypatch.setattr(server, '_workflow_call', dispatch)
    result = await call('resume_workflow', workflow_run_id='run', instructions=ANSWER, decision_id='current', allow_optional_review_skip=allow)
    assert not result.is_error
    assert calls[0]['allow_optional_review_skip'] is allow


@pytest.mark.parametrize("accepted_child", [False, True])
async def test_legacy_pending_forwarded_delivery_replays_without_consent(storage, tmp_path, accepted_child):
    root, child = await forwarded(storage, tmp_path)
    delivery = {'id': 'legacy-delivery', 'payload': {
        'source_id': child['workflow_run_id'], 'action': 'resume', 'instructions': ANSWER,
        'additional_attempts': 0, 'decision_id': child['input_decision_id'],
        'interaction_owner': None, 'source': root['input_source'],
    }}
    storage.update_run(root['workflow_run_id'], lambda r: r.update(forwarded_delivery=delivery), 'fixture')
    if accepted_child:
        def accept(r):
            r.update(status='running', instructions=ANSWER, forwarded_delivery_receipt=delivery)
            r.pop('input_decision_id', None)
            r.pop('input_question', None)
        storage.update_run(child['workflow_run_id'], accept, 'fixture')
    result = storage.control(root['workflow_run_id'], 'resume', instructions=ANSWER, decision_id=child['input_decision_id'])
    assert result['status'] == 'running'
    source = storage.get_run(child['workflow_run_id'])
    assert not source.get('optional_skip_authorizations')
    assert source['forwarded_delivery_receipt'] == delivery
