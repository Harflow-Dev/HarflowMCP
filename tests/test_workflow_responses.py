import json
import pytest
from harflow_mcp.workflow_responses import compact, detail, BUDGET
from harflow_mcp import server
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture
def owned_state(tmp_path):
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        yield root


def test_monitor_history_is_bounded_and_prioritizes_active_runs():
    from harflow_mcp.workflow_responses import history_page
    runs = [large_run() | {"workflow_run_id": str(i), "status": "completed", "created_at": i} for i in range(120)]
    runs.append(large_run() | {"workflow_run_id": "active", "created_at": 0, "repo_path": "/work/repo", "prompt": "Review parser"})
    page = history_page(runs, limit=100)
    assert page["runs"][0]["workflow_run_id"] == "active"
    assert page["runs"][0]["repo_path"] == "/work/repo"
    assert page["runs"][0]["prompt"] == "Review parser"
    assert len(json.dumps(page).encode()) < 260 * 1024
    assert page["next_offset"] is not None
    assert all("raw_output" not in a for r in page["runs"] for a in r["activations"])
    assert all("definition" not in r for r in page["runs"])
    second = history_page(runs, offset=page["next_offset"])
    assert not ({r["workflow_run_id"] for r in page["runs"]} & {r["workflow_run_id"] for r in second["runs"]})


@pytest.mark.parametrize("offset,limit", [(-1, 10), (0, 0), (0, 101), (True, 10)])
def test_monitor_history_rejects_invalid_paging(offset, limit):
    from harflow_mcp.workflow_responses import history_page
    with pytest.raises(ValueError):
        history_page([], offset, limit)


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


async def test_undecidable_workflow_reader_fails_closed(monkeypatch, owned_state):
    from types import SimpleNamespace
    from harflow_mcp import lineage
    async def caller(*args, **kwargs):
        return None
    registry = SimpleNamespace(_detect_caller=caller, log_dir=owned_state / "tasks")
    monkeypatch.setattr(server, '_reg', lambda: registry)
    monkeypatch.setattr(lineage, 'detect_caller_detail', lambda *args, **kwargs: lineage.Detection(None, undecidable='unreadable ancestry'))
    with pytest.raises(Exception, match='undecidable'):
        await server.get_workflow_run_detail('r', 'definition')


async def test_authorized_definition_is_exact_snapshot_launched(monkeypatch, tmp_path):
    from harflow_mcp import workflows
    monkeypatch.setenv('HOME', str(tmp_path))
    snapshot = {'name': 'example', 'revision': 1, 'routing_mode': 'explicit', 'orchestrator': {'backend': 'codex'}, 'nodes': [{'id': 'start', 'type': 'start'}, {'id': 'end', 'type': 'end'}], 'connections': [{'id': 'go', 'source': 'start', 'target': 'end'}]}
    calls = []
    def get(self, name):
        calls.append(name)
        return snapshot
    async def caller():
        return None
    async def validate(path):
        return tmp_path
    async def launch(**kwargs):
        launched = kwargs['definition_snapshot']
        assert launched == workflows.launch_definition(snapshot, None)
        tree = kwargs['dependency_tree']
        assert tree['workflows'][tree['root_workflow_id']]['definition'] == launched
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


def test_builder_compact_revision_and_lossless_draft_views():
    from harflow_mcp.workflow_responses import compact, detail
    run = {"workflow_run_id": "builder", "kind": "builder", "status": "running", "draft_revision": 7, "builder_draft": {"nodes": [{"instructions": "large" * 10000}]}, "generated_definition": {"name": "Saved proposal"}}
    assert compact(run)["draft_revision"] == 7
    assert "builder_draft" not in compact(run)
    for view in ("builder_draft", "generated_definition"):
        chunks, cursor = [], None
        while True:
            response = detail(run, view, cursor=cursor)
            chunks.append(response["chunk"])
            cursor = response["next_cursor"]
            if cursor is None:
                break
        assert json.loads("".join(chunks)) == run[view]


@pytest.mark.parametrize('projection', ['compact', 'monitor'])
def test_actionable_questions_survive_every_budget_pass(projection):
    from harflow_mcp import workflow_responses
    question = 'Choose the compatible behavior.\n' + '🧠' * 30000 + '\n1. Preserve spacing\n2. Keep all choices'
    run = large_run() | {'status': 'needs_input', 'input_question': question,
        'name': '🧠' * 100000, 'reason': '🧠' * 100000,
        'pending_inputs': [{'workflow_run_id': 'child', 'decision_id': 'decision',
            'question': question, 'workflow_name': 'x' * 10000, 'path': ['root', 'child']}],
        'pending_attention': [{'workflow_run_id': str(i), 'reason': 'x' * 10000} for i in range(200)],
        'input_delivery_receipt': {'workflow_run_id': 'child', 'decision_id': 'decision',
            'accepted_at': 12, 'delivery_id': 'delivery', 'private': 'secret'}}
    result = getattr(workflow_responses, projection)(run)
    assert result['input_question'] == question
    assert result['input_question_truncated'] is False
    assert result['question_items'] == ['1. Preserve spacing', '2. Keep all choices']
    assert result['pending_inputs'][0]['question'] == question
    assert result['pending_inputs'][0]['question_items'] == result['question_items']
    assert result['pending_input_count'] == 1
    assert result['pending_attention_count'] == 200
    assert result['input_delivery_receipt'] == {'workflow_run_id': 'child', 'decision_id': 'decision',
        'accepted_at': 12, 'delivery_id': 'delivery'}
    assert workflow_responses._metadata_size(result) <= BUDGET


def test_question_items_do_not_invent_choices():
    from harflow_mcp.workflow_responses import question_items
    assert question_items('Should we preserve compatibility or change it?') == []
    assert question_items('Answer these:\n- First question?\n  * Second question?') == ['- First question?', '  * Second question?']


async def test_status_and_wait_return_full_actionable_questions(monkeypatch):
    question = 'Full question ' * 1000 + '\n1. First\n2. Second'
    async def call(*args, **kwargs):
        return {'workflow_run_id': 'r', 'status': 'needs_input', 'input_question': question}
    monkeypatch.setattr(server, '_workflow_call', call)
    for result in (await server.get_workflow_status('r'), await server.wait_for_workflow('r', 0)):
        assert result['input_question'] == question
        assert result['input_question_truncated'] is False


def test_authored_question_items_survive_without_reinterpretation():
    items = ['First question with punctuation?' + '🧠' * 4000, 'Second question without list marker']
    result = compact({'workflow_run_id': 'r', 'status': 'needs_input',
        'input_question': 'Please answer both questions.', 'input_question_items': items,
        'pending_inputs': [{'question': 'Other exact prose', 'question_items': items}]})
    assert result['input_question_items'] == items
    assert result['pending_inputs'][0]['question_items'] == items
