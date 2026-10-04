"""Saving definitions cannot expand an ordinary caller's capability envelope."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from polybridge import backends, server, workflow_hooks, workflows


def definition(freedom='read_only', network=None):
    worker = {'id': 'work', 'type': 'agent', 'role': 'task', 'agent': {'backend': 'codex'}, 'freedom': freedom}
    if network is not None:
        worker['network'] = network
    return {'name': 'safe', 'orchestrator': {'backend': 'codex'}, 'nodes': [{'id': 'start', 'type': 'start'}, worker, {'id': 'end', 'type': 'end'}], 'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'}, {'id': 'finish', 'source': 'work', 'target': 'end'}]}


def caller(tmp_path, freedom='read_only', network=False):
    enforcement = backends.get('codex').enforcement(freedom, network).as_dict()
    record = SimpleNamespace(task_id='caller', backend='codex', freedom=freedom, network=network, enforcement=enforcement, repo_path=str(tmp_path))
    return SimpleNamespace(record=record)


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(backends, 'is_installed', lambda backend: True)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=tmp_path / '.polybridge' / 'tasks'))
    monkeypatch.setattr(workflow_hooks, 'refuse_managed', lambda *args: None)
    async def managed():
        return None
    monkeypatch.setattr(server, '_managed_workflow_reader', managed)
    return tmp_path


@pytest.mark.parametrize('freedom', ['write_in_repo', 'publish', 'unrestricted'])
async def test_ordinary_read_only_caller_cannot_save_stronger_nodes(isolated, monkeypatch, freedom):
    async def verified():
        return caller(isolated)
    monkeypatch.setattr(server, '_verified_workflow_caller', verified)
    with pytest.raises(Exception, match="cannot exceed the caller's freedom"):
        await server.save_workflow('safe', definition(freedom), 0)
    assert not (isolated / '.polybridge' / 'workflows' / 'safe.json').exists()


async def test_ordinary_caller_cannot_enable_network_beyond_recorded_restriction(isolated, monkeypatch):
    async def verified():
        return caller(isolated, 'write_in_repo', False)
    monkeypatch.setattr(server, '_verified_workflow_caller', verified)
    with pytest.raises(Exception, match="cannot exceed the caller's network"):
        await server.save_workflow('safe', definition('write_in_repo', True), 0)


async def test_ordinary_caller_can_save_same_or_narrower_authority(isolated, monkeypatch):
    async def verified():
        return caller(isolated, 'write_in_repo', False)
    monkeypatch.setattr(server, '_verified_workflow_caller', verified)
    result = await server.save_workflow('safe', definition('read_only', False), 0)
    assert result['nodes'][1]['freedom'] == 'read_only'


async def test_human_save_remains_capable_of_setting_workflow_access(isolated, monkeypatch):
    async def verified():
        return None
    monkeypatch.setattr(server, '_verified_workflow_caller', verified)
    result = await server.save_workflow('safe', definition('unrestricted', True), 0)
    assert result['nodes'][1]['freedom'] == 'unrestricted'
    assert result['nodes'][1]['network'] is True


def test_unrecorded_caller_enforcement_is_not_treated_as_unrestricted(tmp_path, monkeypatch):
    monkeypatch.setattr(backends, 'is_installed', lambda backend: True)
    source = caller(tmp_path, 'unrestricted', None)
    source.record.enforcement = None
    with pytest.raises(ValueError, match='unrecorded'):
        server._guard_saved_workflow_authority(source, workflows.validate_definition(definition()))
