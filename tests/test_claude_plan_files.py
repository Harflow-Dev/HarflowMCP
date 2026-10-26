from pathlib import Path
import pytest
from harflow_mcp.backends.claude_plan_files import observe, plan_roots
from harflow_mcp.backends import get
from harflow_mcp.backends.base import Accumulator


def events(path, *, error=False, tool='Write'):
    return ({'type': 'assistant', 'message': {'content': [{'type': 'tool_use', 'id': 'p1', 'name': tool, 'input': {'file_path': str(path)}}]}},
            {'type': 'user', 'message': {'content': [{'type': 'tool_result', 'tool_use_id': 'p1', 'is_error': error}]}})


def test_plan_write_requires_successful_correlated_result(tmp_path, monkeypatch):
    tmp_path = tmp_path.resolve()
    config = tmp_path / 'config'; plan = config / 'plans/actual.md'
    plan.parent.mkdir(parents=True); plan.write_text('plan')
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(config))
    repo = tmp_path / 'repo'; repo.mkdir()
    acc = Accumulator(); backend = get('claude')
    call, result = events(plan)
    assert backend.observed_plan_files(result, acc, repo) == []
    assert backend.observed_plan_files(call, acc, repo) == []
    found = backend.observed_plan_files(result, acc, repo)
    assert found[0]['path'] == str(plan)
    assert found[0]['registered_observation']['size'] == 4
    assert backend.observed_plan_files(result, acc, repo) == []
    call, result = events(plan, error=True)
    observe(call, {}, repo)
    state = {}; observe(call, state, repo)
    assert observe(result, state, repo) == []


def test_plan_namespace_rejects_regular_repo_config_and_symlinks(tmp_path, monkeypatch):
    tmp_path = tmp_path.resolve()
    config = tmp_path / 'config'; (config / 'plans').mkdir(parents=True)
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(config))
    repo = tmp_path / 'repo'; repo.mkdir()
    for path in (repo / 'file.md', config / 'settings.json', config / 'plans/subdir/file.md'):
        path.parent.mkdir(parents=True, exist_ok=True); path.write_text('x')
        state = {}; call, result = events(path)
        observe(call, state, repo); assert observe(result, state, repo) == []
    outside = tmp_path / 'outside.md'; outside.write_text('x')
    link = config / 'plans/link.md'; link.symlink_to(outside)
    state = {}; call, result = events(link)
    observe(call, state, repo); assert observe(result, state, repo) == []


def test_custom_plan_directory_is_project_contained(tmp_path, monkeypatch):
    tmp_path = tmp_path.resolve()
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(tmp_path / 'config'))
    repo = tmp_path / 'repo'; (repo / '.claude').mkdir(parents=True)
    settings = repo / '.claude/settings.json'
    settings.write_text('{"plansDirectory":"my-plans"}')
    assert repo / 'my-plans' in plan_roots(repo)
    settings.write_text('{"plansDirectory":"../outside"}')
    assert tmp_path / 'outside' not in plan_roots(repo)


def test_read_only_discloses_builtin_plan_exception():
    assert any('CLAUDE_CONFIG_DIR/plans' in caveat and 'plansDirectory' in caveat
               for caveat in get('claude').enforcement('read_only').caveats)


async def test_owner_stream_records_actual_builtin_plan_artifact(tmp_path, monkeypatch):
    """Exercise stdout -> successful tool evidence -> durable artifact page together."""
    import json
    import shlex
    import sys
    from harflow_mcp import artifacts, backends, state_ownership
    from harflow_mcp.tasks import TaskRegistry
    from harflow_mcp.state_layout import resolve_layout
    from test_events import _RecordingBackend
    tmp_path = tmp_path.resolve()
    config = tmp_path / 'config'
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(config))
    repo = tmp_path / 'repo'; repo.mkdir()
    plan = config / 'plans/actual.md'
    plan.parent.mkdir(parents=True)
    call, result = events(plan)
    script = f"from pathlib import Path; import json; Path({str(plan)!r}).write_text('built-in plan'); print({json.dumps(call)!r}, flush=True); print({json.dumps(result)!r}, flush=True)"
    class Backend(_RecordingBackend):
        name = 'plan-stream-double'
        def observed_plan_files(self, event, acc, repo):
            return get('claude').observed_plan_files(event, acc, repo)
        def builtin_plan_path(self, path, repo):
            return get('claude').builtin_plan_path(path, repo)
    backend = Backend(shlex.join([sys.executable, '-c', script]))
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    monkeypatch.setattr(artifacts, '_caller', lambda _: None)
    with state_ownership.state_session(resolve_layout(root=tmp_path / 'state')):
        registry = TaskRegistry(log_dir=tmp_path / 'state/tasks')
        task = await registry.start('make a plan', repo, backend=backend, freedom='read_only')
        await task.done.wait()
        assert task.status == 'completed'
        page = artifacts.get(registry.log_dir, task.task_id)
        assert page['count'] == 1
        item = page['items'][0]
        assert item['path'] == str(plan)
        assert item['provenance']['method'] == 'observed_backend_plan'
        assert item['availability']['state'] == 'available'
