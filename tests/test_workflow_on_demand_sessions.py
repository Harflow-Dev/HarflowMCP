"""Session selection and native reuse for on-demand calls."""
import asyncio
import copy

import pytest

from polybridge import workflows as w
from polybridge.workflow_on_demand import validate_action
import polybridge.workflow_on_demand as od
from test_workflow_delegation import default_decision, graph
from test_workflow_native import MODEL, NativeRegistry
from test_workflow_delegation import Registry


def _run_definition(session_mode="agent_decides"):
    value = graph()
    value["orchestrator"] = {"backend": "claude", "model": MODEL}
    value["on_demand_nodes"] = [{
        "id": "research", "title": "API researcher", "instructions": "Return evidence.",
        "agent": {"backend": "claude", "model": MODEL}, "session_mode": session_mode,
        "execution_mode": "prefer_subagent", "freedom": "read_only",
    }]
    return value


def _action(mode=None, reference=None):
    value = {"action": "call_on_demand", "decision_id": "d1", "node_id": "owner:research", "assignment": "Verify API", "reason": "Need evidence"}
    if mode is not None:
        value["session_mode"] = mode
    if reference is not None:
        value["resume_task_id"] = reference
    return value


def test_agent_decides_omission_requires_explicit_choice_once_a_session_is_issued(monkeypatch):
    from test_workflow_on_demand_contract import MemoryStorage, run_fixture
    run = run_fixture()
    storage = MemoryStorage({"owner": run})
    monkeypatch.setattr(od, "sessions", lambda *_args: [])
    _, mode, reference = validate_action(storage, run, {"decision_id": "d1"}, _action())
    assert (mode, reference) == ("fresh", None)
    issued = [{"task_id": "issued", "execution_id": "old", "session_id": "s1"}]
    monkeypatch.setattr(od, "sessions", lambda *_args: copy.deepcopy(issued))
    with pytest.raises(w.WorkflowError, match="explicit Fresh or Resume"):
        validate_action(storage, run, {"decision_id": "d1"}, _action())
    assert validate_action(storage, run, {"decision_id": "d1"}, _action("fresh"))[1:] == ("fresh", None)
    assert validate_action(storage, run, {"decision_id": "d1"}, _action("resume", "issued"))[1:] == ("resume", "issued")
    with pytest.raises(w.WorkflowError, match="issued compatible"):
        validate_action(storage, run, {"decision_id": "d1"}, _action("resume", "foreign"))


@pytest.mark.parametrize("issued, expected", [(False, "fresh"), (True, "resume")])
def test_fixed_resume_bootstraps_only_without_a_retained_session(monkeypatch, issued, expected):
    from test_workflow_on_demand_contract import MemoryStorage, run_fixture
    run = run_fixture()
    run["definition"]["on_demand_nodes"][0]["session_mode"] = "resume"
    storage = MemoryStorage({"owner": run})
    sessions = [{"task_id": "issued", "execution_id": "old", "session_id": "s1"}] if issued else []
    monkeypatch.setattr(od, "sessions", lambda *_args: copy.deepcopy(sessions))
    action = _action() if not issued else _action("resume", "issued")
    assert validate_action(storage, run, {"decision_id": "d1"}, action)[1] == expected


class RecordingRegistry(Registry):
    def __init__(self, root, policy):
        super().__init__(root, policy)
        self.resumes = []

    async def resume(self, previous, prompt, **kwargs):
        self.resumes.append((previous.task_id, kwargs.get("task_id")))
        return await super().resume(previous, prompt, **kwargs)


class NativeOnDemandRegistry(NativeRegistry):
    def __init__(self, root, policy):
        super().__init__(root)
        self.policy = policy


class ResumeUnavailableRegistry(RecordingRegistry):
    def __init__(self, root, policy):
        super().__init__(root, policy)
        self.specialist_resumes = 0

    async def resume(self, previous, prompt, **kwargs):
        self.resumes.append((previous.task_id, kwargs.get("task_id")))
        if "Execute the specialist assignment." not in prompt:
            return await super(RecordingRegistry, self).resume(previous, prompt, **kwargs)
        self.specialist_resumes += 1
        raise RuntimeError("issued specialist session is unavailable")


async def test_two_native_calls_reuse_the_issued_specialist_session(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    storage = w.WorkflowStore(tmp_path)
    calls = []

    def policy(context, registry):
        if context["current_stage"]["node_id"] == "start" and len(calls) < 2:
            entry = context["on_demand_nodes"][0]
            action = {"decision_id": context["decision_id"], "action": "call_on_demand", "node_id": entry["node_id"], "assignment": f"Verify API pass {len(calls) + 1}", "reason": "Need evidence"}
            if calls:
                sessions = entry["available_sessions"]
                assert sessions
                action.update(session_mode="resume", resume_task_id=sessions[0]["task_id"])
            calls.append(action)
            return action
        return default_decision(context, registry)

    definition = _run_definition()
    definition["orchestrator"] = {"backend": "codex"}
    specialist = definition["on_demand_nodes"][0]
    specialist.update(agent={"backend": "codex"}, execution_mode="headless")
    registry = RecordingRegistry(tmp_path, policy)
    run = storage.create_run(w.validate_definition(definition), "Review API", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason")
    assert len(observed["on_demand_calls"]) == 2
    assert all(item["status"] == "succeeded" for item in observed["on_demand_calls"])
    assert observed["on_demand_calls"][1]["execution_session_mode"] == "resume"
    assert observed["on_demand_calls"][1]["resume_task_id"] == observed["on_demand_calls"][0]["task_id"]
    assert registry.resumes


def test_selected_resume_availability_failure_returns_checkpoint_without_fresh_fallback(monkeypatch):
    from test_workflow_on_demand_contract import MemoryStorage, run_fixture
    run = run_fixture()
    run["definition"]["on_demand_nodes"][0]["session_mode"] = "resume"
    storage = MemoryStorage({"owner": run})
    monkeypatch.setattr(od, "sessions", lambda *_args: [{"task_id": "issued", "execution_id": "old", "session_id": "s1"}])
    entry, mode, reference = validate_action(storage, run, {"decision_id": "d1"}, _action("resume", "issued"))
    assert entry["node_id"] == "owner:research"
    assert (mode, reference) == ("resume", "issued")
    # The selected resume identity is retained for dispatch; no fallback mode is inferred here.
    assert mode != "fresh"


def test_foreign_and_expired_resume_ids_are_rejected_by_retained_session_lookup(tmp_path):
    from polybridge import store
    from test_workflow_on_demand_contract import MemoryStorage, run_fixture

    run = run_fixture()
    run["repo_path"] = str(tmp_path)
    run["activations"] = [{"id": "old", "node_id": "owner:research", "role": "node", "status": "completed", "tasks": [{
        "task_id": "issued", "status": "completed", "repo_path": str(tmp_path), "freedom": "read_only", "network": None,
        "candidate": {"backend": "codex"}, "result": {"session_id": "session-1"},
    }], "node_result": {"status": "succeeded"}}]
    storage = MemoryStorage({"owner": run})
    storage.root = tmp_path
    store.write(tmp_path / "tasks", store.TaskRecord(task_id="issued", backend="codex", session_id="session-1", repo_path=str(tmp_path), started_at="2026-10-18T00:00:00Z", freedom="read_only", network=None, status="completed"))
    action = _action("resume", "issued")
    assert validate_action(storage, run, {"decision_id": "d1"}, action)[2] == "issued"
    for reference in ("foreign", "expired"):
        with pytest.raises(w.WorkflowError, match="issued compatible"):
            validate_action(storage, run, {"decision_id": "d1"}, _action("resume", reference))
    store.record_path(tmp_path / "tasks", "issued").unlink()
    with pytest.raises(w.WorkflowError, match="issued compatible"):
        validate_action(storage, run, {"decision_id": "d1"}, action)


async def test_selected_resume_failure_returns_checkpoint_without_silent_fresh_dispatch(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    storage = w.WorkflowStore(tmp_path)
    calls = []

    def policy(context, registry):
        if context["current_stage"]["node_id"] == "start" and len(calls) < 2:
            entry = context["on_demand_nodes"][0]
            action = {"decision_id": context["decision_id"], "action": "call_on_demand", "node_id": entry["node_id"], "assignment": f"Verify API pass {len(calls) + 1}", "reason": "Need evidence"}
            if calls:
                action.update(session_mode="resume", resume_task_id=entry["available_sessions"][0]["task_id"])
            calls.append(action)
            return action
        return default_decision(context, registry)

    definition = _run_definition()
    definition["orchestrator"] = {"backend": "codex"}
    definition["on_demand_nodes"][0].update(agent={"backend": "codex"}, execution_mode="headless")
    registry = ResumeUnavailableRegistry(tmp_path, policy)
    run = storage.create_run(w.validate_definition(definition), "Review API", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention", observed.get("attention_reason")
    assert len(observed["on_demand_calls"]) == 2
    assert observed["on_demand_calls"][1]["execution_session_mode"] == "resume"
    assert observed["on_demand_calls"][1]["status"] == "uncertain"
    assert registry.specialist_resumes == 1


async def test_specialist_primary_unavailable_uses_fallback_once_and_returns_result(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: backend.name != "opencode")
    monkeypatch.setattr(w.backends, "version", lambda backend: "2.1.295 (Claude Code)")
    storage = w.WorkflowStore(tmp_path)
    definition = _run_definition()
    definition["on_demand_nodes"][0]["agent"] = {"backend": "opencode", "fallbacks": [{"backend": "claude", "model": MODEL}]}
    calls = {"count": 0}

    def policy(context, registry):
        if context["current_stage"]["node_id"] == "start" and calls["count"] == 0:
            calls["count"] += 1
            entry = context["on_demand_nodes"][0]
            return {"decision_id": context["decision_id"], "action": "call_on_demand", "node_id": entry["node_id"], "assignment": "Verify API", "reason": "Need evidence"}
        return default_decision(context, registry)

    registry = NativeOnDemandRegistry(tmp_path, policy)
    run = storage.create_run(w.validate_definition(definition), "Review API", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason")
    assert len(observed["on_demand_calls"]) == 1
    specialist = next(a for a in observed["activations"] if a.get("on_demand_call_id"))
    assert specialist["tasks"][0]["candidate"]["backend"] == "claude"
    assert specialist["execution_kind"] == "native_subagent"
    assert len(registry.native_calls) == 1

