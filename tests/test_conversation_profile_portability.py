"""Model-free capture of actual registry profiles before agent process creation."""
from copy import deepcopy
import json
import uuid

import pytest

from harflow_mcp import backends, state_ownership, tasks
from harflow_mcp.backends.conversation_native import conversation_profile, VERSIONS
from harflow_mcp.backends.conversation_qualification import canonical_profile, fingerprint
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry


class Captured(Exception):
    pass


class Capture:
    def __init__(self):
        self.before = self.final = self.policy = self.allocation = None

    def preflight(self, backend, invocation, policy):
        self.before = deepcopy(invocation)
        return True

    def finalize(self, preflight, backend, invocation, policy):
        self.final, self.policy = deepcopy(invocation), deepcopy(policy)
        raise Captured


async def capture(registry, backend, repo, freedom, *, task_id=None):
    factory = Capture()
    registry._conversation_launch_factory = factory
    allocate = registry._allocate_shared_scratch
    async def measured_allocation(invocation, **kwargs):
        result = await allocate(invocation, **kwargs)
        factory.allocation = deepcopy(result[2])
        return result
    registry._allocate_shared_scratch = measured_allocation
    try:
        with pytest.raises(Captured):
            await registry.start('literal input', repo, backend=backend, freedom=freedom, task_id=task_id)
    finally:
        registry._allocate_shared_scratch = allocate
    return factory


def profile(backend, invocation, repo, freedom):
    result = conversation_profile(backend, invocation, version=VERSIONS[backend.name], freedom=freedom,
        profile_name='shipped-medium35-high' if backend.name == 'vibe' else None)
    result.setdefault('cwd', str(repo))
    result['requested_network'] = None
    return result


@pytest.fixture
def tmp_path(tmp_path_factory):
    root = tmp_path_factory.mktemp('profile-portability')
    with state_ownership.state_session(resolve_layout(root=root / 'state')):
        (root / 'state' / 'tasks').mkdir()
        yield root


@pytest.mark.parametrize('name', ['claude', 'codex', 'vibe', 'antigravity', 'cursor'])
@pytest.mark.parametrize('freedom', ['read_only', 'write_in_repo'])
async def test_actual_preflight_and_final_profile_without_agent_spawn(tmp_path, monkeypatch, name, freedom):
    async def forbidden(*args, **kwargs):
        pytest.fail('profile audit must stop before subprocess creation')
    monkeypatch.setattr(tasks.asyncio, 'create_subprocess_exec', forbidden)
    backend = backends.get(name)
    registry = TaskRegistry(log_dir=tmp_path / 'state' / 'tasks')
    captured = await capture(registry, backend, tmp_path, freedom)
    before, final = captured.before, captured.final
    result = profile(backend, final, tmp_path, freedom)
    canonical = canonical_profile(backend, final, result, repo_path=tmp_path)
    assert canonical['profile']['cwd'] == {'harflow_path_role': 'repository'}
    if name in ('claude', 'codex') and freedom == 'write_in_repo':
        assert before.scratch_directory is None and not before.shared_scratch_directories
        assert final.scratch_directory is not None and final.shared_scratch_directories
        assert set(captured.allocation.access.values()) == {'writable'}
        assert not {'state_root', 'managed_allocation', 'chain_id', 'group_name'} & captured.policy.keys()
        assert str(tmp_path / 'state') in json.dumps(canonical)
    else:
        assert final.scratch_directory is None and not final.shared_scratch_directories
    if name in ('vibe', 'antigravity', 'cursor'):
        assert before == final  # Allocated scratch has no additional backend grant.
        if freedom == 'write_in_repo':
            assert set(captured.allocation.access.values()) == {'no_additional_grant'}
            assert captured.allocation.writable == []
            assert list((tmp_path / 'state' / 'shared-scratch' / 'chain').iterdir())


@pytest.mark.parametrize('name', ['claude', 'codex'])
async def test_automatic_managed_grants_currently_change_scope_per_task_uuid(tmp_path, name):
    backend = backends.get(name)
    registry = TaskRegistry(log_dir=tmp_path / 'state' / 'tasks')
    one = await capture(registry, backend, tmp_path, 'write_in_repo', task_id=str(uuid.uuid4()))
    two = await capture(registry, backend, tmp_path, 'write_in_repo', task_id=str(uuid.uuid4()))
    def scope(captured):
        return canonical_profile(backend, captured.final,
            profile(backend, captured.final, tmp_path, 'write_in_repo'), repo_path=tmp_path)
    first, second = scope(one), scope(two)
    assert fingerprint(first) != fingerprint(second)
    assert first['invocation_authority']['scratch_directory'] != second['invocation_authority']['scratch_directory']
