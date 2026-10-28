import asyncio
import json
from pathlib import Path
import pytest
from harflow_mcp import approval_paths, artifacts, retention, scratch, state_ownership, store
from harflow_mcp.state_layout import resolve_layout


def test_outputs_names_concurrency_resume_and_refuse_overwrite(tmp_path):
    root = tmp_path.resolve() / 'state'
    with state_ownership.state_session(resolve_layout(root=root)):
        log = root / 'tasks'; log.mkdir()
        for task_id in ('review-one', 'review-two', 'review-resumed'):
            result = json.dumps({'result': {'technical_plan': 'revised plan'}})
            paths = scratch.materialize_outputs(log, task_id, result)
            assert [p.name for p in paths] == [f'report-{task_id}.md', f'plan-{task_id}.md']
            assert paths[1].read_text() == 'revised plan'
            with pytest.raises(FileExistsError):
                scratch.materialize_outputs(log, task_id, 'different')
            assert paths[0].read_text() == result
        with pytest.raises(ValueError):
            scratch.materialize_outputs(log, '../escape', 'report')
        scratch.remove(log, 'review-one')
        assert not scratch.output_directory(log, 'review-one').exists()
        assert scratch.output_directory(log, 'review-two').exists()


def test_outputs_refuse_symlink(tmp_path):
    root = tmp_path.resolve() / 'state'
    with state_ownership.state_session(resolve_layout(root=root)):
        log = root / 'tasks'; log.mkdir()
        outside = tmp_path / 'outside'; outside.mkdir()
        (root / 'outputs').symlink_to(outside)
        with pytest.raises(ValueError, match='symlink'):
            scratch.materialize_outputs(log, 'review', 'report')
        assert list(outside.iterdir()) == []


def test_only_plain_project_plans_exempt_from_configuration_protection(tmp_path):
    repo = tmp_path.resolve() / 'repo'; repo.mkdir()
    config = tmp_path / 'config'
    targets = approval_paths.protected_targets(repo, tmp_path / 'state/tasks', environ={'HOME': str(tmp_path), 'CLAUDE_CONFIG_DIR': str(config)})
    plans = repo / '.claude/plans'; plans.mkdir(parents=True)
    plan = plans / 'task.md'
    assert approval_paths.protection(targets, 'Write', str(plan)) is None
    plan.write_text('first'); plan.write_text('updated')
    assert approval_paths.protection(targets, 'Edit', str(plan)) is None
    for relative in ('.claude/settings.json', '.claude/settings.local.json', '.claude/hooks/x', '.claude/agents/x', '.claude/commands/x', '.claude/skills/x', '.claude/unknown/x', '.mcp.json'):
        assert approval_paths.protection(targets, 'Write', str(repo / relative)) == 'protected_path'
    escape = plans / 'escape.md'; escape.symlink_to(repo / '.claude/settings.json')
    assert approval_paths.protection(targets, 'Write', str(escape)) == 'protected_path'
    alias = plans / 'alias'; alias.symlink_to(tmp_path)
    # A symlink out of plans remains protected by the lexical project namespace.
    assert approval_paths.protection(targets, 'Write', str(alias / 'other.md')) == 'protected_path'


async def test_read_only_final_output_is_discoverable_without_write_grants(tmp_path, monkeypatch):
    import shlex
    import sys
    from harflow_mcp import backends
    from harflow_mcp.tasks import TaskRegistry
    from test_events import _RecordingBackend
    tmp_path = tmp_path.resolve()
    event = {'type': 'result', 'subtype': 'success', 'is_error': False, 'result': 'review report'}
    class Backend(_RecordingBackend):
        name = 'output-double'
        def ingest(self, event, acc):
            backends.get('claude').ingest(event, acc)
    script = 'import json; print(' + repr(json.dumps(event)) + ')'
    backend = Backend(shlex.join([sys.executable, '-c', script]))
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    with state_ownership.state_session(resolve_layout(root=tmp_path / 'state')):
        registry = TaskRegistry(log_dir=tmp_path / 'state/tasks')
        task = await registry.start('review', tmp_path, backend=backend, freedom='read_only')
        await task.monitor
        monkeypatch.setattr(artifacts, '_caller', lambda _: None)
        page = artifacts.get(registry.log_dir, task.task_id)
        output = next(item for item in page['items'] if item['provenance']['method'] == 'engine_output')
        assert output['availability']['state'] == 'available'
        assert Path(output['path']).read_text() == 'review report'
        assert not any('/outputs/' in path for path in task.enforcement.get('writable_roots', []))
        assert task.status == 'completed'


def test_protected_write_refusal_names_alternative():
    from harflow_mcp.backends.claude_approvals import encode
    request = {'system_rejection': 'protected_path', 'native_request_id': 'request'}
    message = json.loads(encode(request, 'reject'))['response']['response']['message']
    assert 'protected_path' in message and 'HARFLOW_SCRATCH' in message
    assert 'result.technical_plan' in message
