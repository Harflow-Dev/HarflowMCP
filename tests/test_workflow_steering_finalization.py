"""Legacy finalization and explicit admission share one observable save boundary."""
import json
import uuid
from types import SimpleNamespace

import pytest

from harflow_mcp import workflow_steering as steering, workflows
from harflow_mcp.workflow_builder_finalization import finish_ordinary_builder


def definition():
    return {"name": "example", "orchestrator": {"backend": "claude"}, "nodes": [
        {"id": "start", "type": "start"},
        {"id": "end", "type": "end"}],
        "connections": [{"id": "done", "source": "start", "target": "end"}]}


@pytest.fixture
def active(tmp_path, monkeypatch):
    storage = workflows.WorkflowStore(tmp_path / 'state')
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    monkeypatch.setattr(steering, '_transport', lambda *args: None)
    run = storage.create_run(definition(), 'Build', tmp_path, kind='builder', freedom='read_only', interaction_owner='monitor')
    aid = 'old-activation'
    def setup(r):
        r.update(status='running', builder_draft=definition(), draft_revision=0,
                 activations=[{'id': aid, 'node_id': 'builder', 'role': 'builder',
                               'status': 'running', 'created_at': 1,
                               'tasks': [{'task_id': 'old-task', 'status': 'running'}]}])
    storage.update_run(run['workflow_run_id'], setup, 'fixture')
    supervisor = workflows.WorkflowSupervisor(SimpleNamespace(log_dir=storage.root / "tasks", _log_dir=storage.root / "tasks"), storage)
    supervisor.run_id = run['workflow_run_id']
    return storage, supervisor, {'id': aid}


def request(storage, rid):
    run = storage.get_run(rid)
    return {'schema_version': 1, 'request_id': str(uuid.uuid4()), 'action': 'open',
            'expected_interaction_epoch': run['interaction_epoch'],
            'expected_control_revision': run['steering']['control_revision'],
            'episode_id': None, 'expected_steering_revision': None,
            'target': steering.target(run), 'text': 'Discuss before saving.', 'allow_fresh_context': True}


async def test_admitted_steering_suppresses_old_initial_autosave(active):
    storage, supervisor, activation = active
    rid = supervisor.run_id
    reply = await steering.steer_workflow(rid, request(storage, rid), root=storage.root, monitor=True)
    assert reply['state'] == 'queued_boundary'
    raw = json.dumps(definition())
    def settled(r):
        r['activations'][0]['tasks'][0].update(status='completed', result={'summary': raw})
    storage.update_run(rid, settled, 'fixture-observed-exit')
    finish_ordinary_builder(supervisor, activation, {'summary': raw})
    run = storage.get_run(rid)
    assert not list(storage.definitions.glob('*.json'))
    assert 'generated_definition' not in run
    assert run['activations'][0]['steering_superseded'] is True
    assert run['activations'][0]['tasks'][0]['result']['summary'] == raw
    assert run['steering']['episodes'][0]['hold'] is True


async def test_completed_legacy_save_makes_old_steering_snapshot_stale(active):
    storage, supervisor, activation = active
    rid = supervisor.run_id
    old = request(storage, rid)
    finish_ordinary_builder(supervisor, activation, {'summary': json.dumps(definition())})
    saved = storage.get('example')
    assert saved['revision'] == 1
    with pytest.raises(steering.SteeringError) as error:
        await steering.steer_workflow(rid, old, root=storage.root, monitor=True)
    assert error.value.code == 'steering_target_stale'
    assert storage.get('example') == saved
    assert storage.get_run(rid)['steering']['turns'] == []


def test_mutation_result_matches_fresh_steering_projection(active):
    storage, supervisor, _ = active
    result = storage.update_run(supervisor.run_id, lambda r: r.update(instructions="same state"), 'fixture')
    assert result == storage.get_run(supervisor.run_id, refresh_atomic=True)
    assert result['steering_summary']['state'] == 'idle'
    assert result['pending_steering_count'] == 0
