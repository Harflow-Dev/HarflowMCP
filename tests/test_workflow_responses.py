import json
import pytest
from polybridge.workflow_responses import compact, detail, BUDGET
from polybridge import server


def large_run():
    return {"workflow_run_id": "r", "name": "Review", "status": "running", "definition": {"huge": "abc🧠" * 90000}, "activations": [{"id": str(i), "status": "completed", "raw_output": "x" * 100000, "result_error": "bad" * 10000} for i in range(10)], "input_question": "🧠" * 90000}


def test_compact_large_run_does_not_leak_output_or_graph():
    result = compact(large_run())
    assert len(json.dumps(result).encode()) <= BUDGET
    assert "definition" not in result and "activations" not in result
    assert result["input_question_truncated"]


def test_complete_oversized_item_is_reconstructed_and_cursor_bound():
    run = large_run()
    chunks = []
    cursor = None
    while True:
        page = detail(run, "definition", cursor)
        assert len(json.dumps(page).encode()) <= BUDGET
        chunks.append(page["chunk"])
        cursor = page["next_cursor"]
        if not cursor:
            break
    assert json.loads("".join(chunks)) == run["definition"]
    cursor = detail(run, "definition")["next_cursor"]
    with pytest.raises(ValueError, match="stale"):
        detail({**run, "workflow_run_id": "other"}, "definition", cursor)
    run["definition"]["new"] = True
    with pytest.raises(ValueError, match="stale"):
        detail(run, "definition", cursor)


async def test_wait_zero_returns_compact_explicit_timeout(monkeypatch):
    async def call(*args, **kwargs):
        return large_run()
    monkeypatch.setattr(server, "_workflow_call", call)
    result = await server.wait_for_workflow("r", 0)
    assert result["timed_out"] and result["effective_timeout_seconds"] == 0
    assert "definition" not in result
    full = await server._wait_workflow_full("r", 0)
    assert full["definition"] == large_run()["definition"]


async def test_detail_authorization_is_not_bypassed_by_projection(monkeypatch):
    async def reader():
        return ({"role": "node"}, {"workflow_run_id": "r"})
    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    with pytest.raises(Exception, match="Worker nodes"):
        await server.get_workflow_run_detail("r", "definition")


@pytest.mark.parametrize("entry", [server.start_workflow, server.start_task])
async def test_workflow_entry_rejects_freedom_before_dispatch(entry):
    args = {"name": "missing"} if entry == server.start_workflow else {"workflow": "missing"}
    with pytest.raises(Exception, match="saved nodes"):
        await entry(prompt="test", repo_path="/tmp", freedom="unrestricted", **args)


async def test_undecidable_workflow_reader_fails_closed(monkeypatch):
    from types import SimpleNamespace
    from polybridge import lineage
    async def caller():
        return None
    registry = SimpleNamespace(_detect_caller=caller, log_dir=__import__('pathlib').Path('/tmp/tasks'))
    monkeypatch.setattr(server, '_reg', lambda: registry)
    monkeypatch.setattr(lineage, 'detect_caller_detail', lambda *args: lineage.Detection(None, undecidable='unreadable ancestry'))
    with pytest.raises(Exception, match='undecidable'):
        await server.get_workflow_run_detail('r', 'definition')


async def test_authorized_definition_is_exact_snapshot_launched(monkeypatch, tmp_path):
    from polybridge import workflows
    monkeypatch.setenv('HOME', str(tmp_path))
    snapshot = {'name': 'example', 'revision': 1, 'nodes': [], 'orchestrator': {'backend': 'codex'}}
    calls = []
    def get(self, name):
        calls.append(name)
        return snapshot
    async def caller():
        return None
    async def validate(path):
        return tmp_path
    async def launch(**kwargs):
        assert kwargs['definition_snapshot'] is snapshot
        return {'workflow_run_id': 'r', 'status': 'pending'}
    monkeypatch.setattr(workflows.WorkflowStore, 'get', get)
    monkeypatch.setattr(workflows, 'start_workflow', launch)
    monkeypatch.setattr(server, '_verified_workflow_caller', caller)
    monkeypatch.setattr(server, '_validate_repo_path', validate)
    await server.start_workflow('example', 'test', str(tmp_path))
    assert calls == ['example']


def test_compact_hostile_unicode_identity_and_questions_stay_bounded():
    run = large_run() | {key: '🧠' * 100000 for key in ('name', 'revision', 'reason', 'wait_reason', 'interaction_owner')}
    assert len(json.dumps(compact(run)).encode()) <= BUDGET


async def test_workflow_list_pagination_respects_total_byte_budget(monkeypatch):
    async def call(*args, **kwargs):
        return [large_run() | {'workflow_run_id': str(i)} for i in range(100)]
    monkeypatch.setattr(server, '_workflow_call', call)
    seen = []
    offset = 0
    while True:
        page = await server.list_workflow_runs(offset)
        assert len(json.dumps(page).encode()) <= BUDGET
        seen.extend(run['workflow_run_id'] for run in page['runs'])
        offset = page['next_offset']
        if offset is None:
            break
    assert seen == [str(i) for i in range(100)]


async def test_in_memory_mcp_client_short_hold_reports_timeout(monkeypatch):
    from mcp import Client
    async def call(*args, **kwargs):
        return {'workflow_run_id': 'r', 'status': 'running'}
    monkeypatch.setattr(server, '_workflow_call', call)
    async with Client(server.mcp) as client:
        response = await client.call_tool('wait_for_workflow', {'workflow_run_id': 'r', 'timeout_seconds': 1})
    result = response.structured_content
    assert result['timed_out']
    assert result['requested_timeout_seconds'] == result['effective_timeout_seconds'] == 1
    assert result['response_version'] == 1
