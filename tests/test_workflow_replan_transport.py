"""Restricted replan transport shares authority and preserves exact request bytes."""
import json
import uuid
from types import SimpleNamespace

import pytest

from harflow_mcp import ctl, server, state_ownership
from harflow_mcp.state_layout import resolve_layout
from test_workflow_replan_faults import pending_replan, replan_request
from test_worker_steering_faults import worker_origin


@pytest.fixture(autouse=True)
def owned_state(tmp_path, monkeypatch):
    root = tmp_path / 'state'
    monkeypatch.setenv('HARFLOW_STATE_ROOT', str(root))
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=root / 'tasks'))
    with state_ownership.state_session(resolve_layout(root=root)):
        pass


def test_ctl_replan_preserves_exact_request_and_human_channel(tmp_path, monkeypatch, capsys):
    from harflow_mcp import workflow_replan
    payload = {'schema_version': 2, 'action': 'replan', 'reason': '  Withdraw this exact launch.\r\nKeep the assignment. 🛠  '}
    path = tmp_path / 'request.json'
    path.write_bytes(json.dumps(payload, ensure_ascii=False).encode())
    calls = []
    async def call(run_id, request, **options):
        calls.append((run_id, request, options))
        return {'accepted': True, 'phase': 'cancel_requested'}
    monkeypatch.setattr(workflow_replan, 'replan_workflow_steering', call)
    assert ctl.main(['workflow-steering-replan', 'root', '--request-file', str(path), '--monitor', '--json']) == 0
    assert calls == [('root', payload, {'root': tmp_path / 'state', 'monitor': True})]
    assert json.loads(capsys.readouterr().out)['result']['phase'] == 'cancel_requested'


@pytest.mark.parametrize('raw', [b'{"reason":"a","reason":"b"}', b'{"value":NaN}', b'[]', b'\xff', b' ' * (128 * 1024 + 1)])
def test_ctl_replan_rejects_invalid_transport_before_service(tmp_path, monkeypatch, capsys, raw):
    from harflow_mcp import workflow_replan
    async def unexpected(*args, **kwargs):
        pytest.fail('Invalid request must not enter the replan service')
    monkeypatch.setattr(workflow_replan, 'replan_workflow_steering', unexpected)
    path = tmp_path / 'bad.json'
    path.write_bytes(raw)
    assert ctl.main(['workflow-steering-replan', 'root', '--request-file', str(path), '--json']) == 1
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'steering_invalid_request'


async def test_mcp_replan_never_asserts_human_channel(monkeypatch, tmp_path):
    from harflow_mcp import workflow_replan
    calls = []
    async def call(run_id, request, **options):
        calls.append((run_id, request, options))
        return {'accepted': True}
    monkeypatch.setattr(workflow_replan, 'replan_workflow_steering', call)
    await server.replan_workflow_steering('root', {'action': 'replan'})
    assert calls == [('root', {'action': 'replan'}, {'root': tmp_path / 'state'})]


async def test_mcp_replan_preserves_typed_refusal(monkeypatch):
    from mcp import MCPError
    from harflow_mcp import workflow_replan, workflow_steering
    async def refuse(*args, **kwargs):
        raise workflow_steering.SteeringError('replan_settlement_unsupported', 'Issued work has no accepted replacement proof')
    monkeypatch.setattr(workflow_replan, 'replan_workflow_steering', refuse)
    with pytest.raises(MCPError) as error:
        await server.replan_workflow_steering('root', {'action': 'replan'})
    assert error.value.error.data['code'] == 'replan_settlement_unsupported'


async def test_mcp_real_normal_control_denial_has_typed_code_and_no_withdrawal(pending_replan):
    from mcp import MCPError
    from harflow_mcp import workflow_steering
    storage, rid, _ = pending_replan
    request = replan_request(storage, rid)
    storage.update_run(rid, lambda run: run.update(interaction_owner='monitor'), 'fixture_interaction_owner')
    observed = workflow_steering.get_workflow_steering(rid, root=storage.root, monitor=True)
    await workflow_steering.steer_workflow(rid, {
        'schema_version': 2, 'ledger_epoch': observed['ledger_epoch'], 'request_id': str(uuid.uuid4()),
        'action': 'release_owner', 'expected_interaction_epoch': observed['interaction_epoch'],
        'expected_control_revision': observed['control_revision'], 'reason': 'Return steering to its original caller.',
    }, root=storage.root, monitor=True)
    before = storage.get_run(rid, refresh_atomic=True)['steering']['requests']
    with pytest.raises(MCPError) as error:
        await server.replan_workflow_steering(rid, request)
    assert error.value.error.data['code'] == 'replan_not_owner'
    assert storage.get_run(rid, refresh_atomic=True)['steering']['requests'] == before
    assert not list((storage.root / 'tasks').glob('*.withdraw.json'))
