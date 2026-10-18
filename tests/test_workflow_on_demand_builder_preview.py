import asyncio
import copy
import json

import pytest

from polybridge import lineage, store, workflows


def connected_graph():
    return {
        "name": "example",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start", "position": {"x": 10, "y": 20}},
            {"id": "work", "type": "agent", "instructions": "Work", "agent": {"backend": "codex"}, "position": {"x": 100, "y": 20}},
            {"id": "end", "type": "end", "position": {"x": 200, "y": 20}},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
        "on_demand_nodes": [
            {"id": "research", "title": "Research", "agent": {"backend": "codex"}},
            {"id": "review", "title": "Review", "instructions": "   ", "agent": {"backend": "opencode"}, "freedom": "read_only"},
        ],
    }


@pytest.fixture
def preview_storage(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda _backend: True)
    monkeypatch.setattr(workflows, "_require_delegation_control", lambda _run: None)
    return workflows.WorkflowStore(tmp_path)


def active_builder(storage, tmp_path):
    run = storage.create_run(
        {"name": "example", "orchestrator": {"backend": "codex"}, "nodes": [], "connections": []},
        "Build",
        tmp_path,
        kind="builder",
        freedom="read_only",
    )
    record = store.TaskRecord(task_id="builder-task", backend="codex", session_id="session", repo_path=str(tmp_path), started_at=1, status="running")
    store.write(storage.root / "tasks", record)
    activation = {"id": "activation", "node_id": "builder", "role": "builder", "status": "running", "tasks": [{"task_id": record.task_id, "status": "running"}]}
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", activations=[activation], draft_revision=0), "test_active")
    return run["workflow_run_id"], lineage.Caller(record, "ancestry")


class FakeTask:
    def __init__(self, task_id, summary):
        self.task_id = task_id
        self.done = asyncio.Event()
        self.done.set()
        self._snapshot = {"task_id": task_id, "session_id": "session", "status": "completed", "summary": summary, "backend": "codex", "stderr_tail": []}

    def snapshot(self):
        return self._snapshot


class FakeRegistry:
    def __init__(self, root, response):
        self._log_dir = root / "tasks"
        self.response = response
        self.tasks = {}

    async def start(self, _prompt, _repo_path, **kwargs):
        task = FakeTask(kwargs["task_id"], self.response)
        self.tasks[task.task_id] = task
        return task


def test_builder_preview_preserves_connected_graph_and_blank_specialists(preview_storage, tmp_path, monkeypatch):
    monkeypatch.setattr(workflows, "_launch", lambda *_args: None)
    captured = connected_graph()
    run = asyncio.run(workflows.build_workflow("example", "Edit specialists", tmp_path, agent={"backend": "codex"}, definition=captured, root=preview_storage.root))
    draft = preview_storage.get_run(run["workflow_run_id"])["builder_draft"]
    assert [node["id"] for node in draft["nodes"]] == ["start", "work", "end"]
    assert [(edge["id"], edge["source"], edge["target"]) for edge in draft["connections"]] == [
        (edge["id"], edge["source"], edge["target"]) for edge in captured["connections"]
    ]
    assert [node["position"] for node in draft["nodes"]] == [node["position"] for node in captured["nodes"]]
    assert draft["nodes"][1]["agent"]["backend"] == "codex"
    assert [(node["id"], node["instructions"]) for node in draft["on_demand_nodes"]] == [("research", ""), ("review", "   ")]
    assert draft["on_demand_nodes"][1]["agent"]["backend"] == "opencode"


@pytest.mark.parametrize(
    "specialist, message",
    [
        ({"id": "research", "instructions": 1}, "text"),
        ({"id": "research", "instructions": "x", "unexpected": True}, "only"),
        ({"id": "work", "instructions": "x"}, "Duplicate"),
        ({"id": "research", "instructions": "x", "freedom": "bad"}, "freedom"),
        ({"id": "research", "instructions": "x", "network": "yes"}, "freedom"),
        ({"id": "research", "instructions": "x", "session_mode": "bad"}, "session_mode"),
        ({"id": "research", "instructions": "x", "execution_mode": "bad"}, "execution_mode"),
    ],
)
def test_builder_preview_rejects_invalid_specialist_shapes(specialist, message):
    graph = connected_graph()
    graph["on_demand_nodes"] = [specialist]
    with pytest.raises(workflows.WorkflowError, match=message):
        workflows.validate_builder_preview(graph, "example")


def test_authenticated_live_builder_apply_advances_once_and_keeps_graph(preview_storage, tmp_path):
    run_id, caller = active_builder(preview_storage, tmp_path)
    draft = connected_graph()
    applied = asyncio.run(workflows.apply_workflow_draft(draft, 0, caller=caller, root=preview_storage.root))
    assert applied["draft_revision"] == 1
    assert [node["id"] for node in applied["builder_draft"]["nodes"]] == ["start", "work", "end"]
    assert [node["id"] for node in applied["builder_draft"]["on_demand_nodes"]] == ["research", "review"]
    with pytest.raises(workflows.WorkflowError, match="revision conflict"):
        asyncio.run(workflows.apply_workflow_draft(draft, 0, caller=caller, root=preview_storage.root))
    assert preview_storage.get_run(run_id)["draft_revision"] == 1


def test_strict_save_rejects_blank_specialist(preview_storage):
    with pytest.raises(workflows.WorkflowError, match="nonempty"):
        preview_storage.save("example", connected_graph())
    assert preview_storage.list() == []


@pytest.mark.parametrize("instructions, expected_status", [("", "needs_attention"), ("Return evidence", "completed")])
def test_builder_finalization_is_strict_but_completed_proposal_succeeds(preview_storage, tmp_path, monkeypatch, instructions, expected_status):
    baseline = copy.deepcopy(connected_graph())
    baseline["on_demand_nodes"][0]["instructions"] = "Existing evidence"
    baseline["on_demand_nodes"][1]["instructions"] = "Existing review guidance"
    baseline = preview_storage.save("example", baseline)
    captured = connected_graph()
    source = {"name": "example", "revision": baseline["revision"], "saved_definition": baseline}
    monkeypatch.setattr(workflows, "_launch", lambda *_args: None)
    run = asyncio.run(workflows.build_workflow("example", "Refine specialists", tmp_path, agent={"backend": "codex"}, definition=captured, source=source, root=preview_storage.root))
    proposal = connected_graph()
    proposal["on_demand_nodes"][0]["instructions"] = instructions
    proposal["on_demand_nodes"][1]["instructions"] = "Review the evidence"
    registry = FakeRegistry(preview_storage.root, json.dumps(proposal))
    asyncio.run(workflows.WorkflowSupervisor(registry, preview_storage).build(run["workflow_run_id"]))
    observed = preview_storage.get_run(run["workflow_run_id"])
    assert observed["status"] == expected_status
    assert preview_storage.get("example") == baseline
    if expected_status == "needs_attention":
        assert "Workflow proposal was invalid" in observed["attention_reason"]
        assert "generated_definition" not in observed
    else:
        assert observed["generated_definition"]["on_demand_nodes"][0]["instructions"] == instructions
