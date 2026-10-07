"""Deterministic child conversation inheritance and strict first-dispatch fixtures."""
import asyncio
import copy
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from polybridge import store as tasks
from polybridge import workflow_child_sessions as reuse
from polybridge import workflow_invocation as inv
from polybridge import workflows as w
from test_workflow_delegation import Task
from test_workflow_run_node_recovery import seed_parent, store, write_run
from test_workflow_run_node_execution import TreeRegistry


@pytest.fixture
def source(store, tmp_path):
    definition = store.get("child")
    definition["orchestrator"].update(model="gpt-6.1-sol", reasoning_effort="high")
    store.save("child", definition, expected_revision=definition["revision"])
    parent = seed_parent(store, tmp_path, "parent", stage="settled")
    old = parent["activations"][0]
    old["status"] = "completed"
    node = next(n for n in parent["definition"]["nodes"] if n["id"] == "call")
    node["child_session_policy"] = "resume"
    child = inv.child_run_record(store, parent, old, node, old["invocation"])
    tid, sid = uuid.uuid4().hex, "retained-orchestrator"
    candidate = child["definition"]["orchestrator"]
    binding = {"task_id": tid, "session_id": sid, "candidate": "orchestrator:" + w._candidate_key(candidate)}
    child.update(status="completed", sessions={"orchestrator": binding}, tasks=[{"id": "old-checklist", "status": "completed"}], decisions=[{"action": "complete"}], transitions=7)
    child["activations"] = [{"id": "old-decision", "role": "orchestrator", "tasks": [{"task_id": tid, "status": "completed", "candidate": copy.deepcopy(candidate)}], "status": "completed"}]
    record = tasks.TaskRecord(task_id=tid, backend="codex", session_id=sid, repo_path=str(tmp_path), started_at="2026-10-07T00:00:00Z", freedom="read_only", network=None, model="gpt-6.1-sol", reasoning_effort="high", status="completed", exit_code=0)
    tasks.write(store.root / "tasks", record)
    write_run(store, child)
    current = copy.deepcopy(old)
    current.update(id=uuid.uuid4().hex, status="running", tasks=[], token={"id": "new-token", "node_id": "call", "stack": [], "input_result_refs": [], "assignment_prompt": "New assignment"})
    current["invocation"] = {**current["invocation"], "child_workflow_run_id": uuid.uuid4().hex, "stage": "preparing"}
    current["token"]["execution_activation_id"] = current["id"]
    parent.update(activations=[old, current], pending=[copy.deepcopy(current["token"])])
    write_run(store, parent)
    return parent, node, old, child, current, record


def selection(store, source):
    parent, node, _, _, current, _ = source
    return reuse.select(store, parent, node, current, current["token"])


def test_resume_builds_fresh_scope_with_separate_inherited_binding(store, source):
    parent, node, _, previous, current, _ = source
    selected = selection(store, source)
    current["invocation"]["child_session_selection"] = selected
    child = inv.child_run_record(store, parent, current, node, current["invocation"])
    assert child["workflow_run_id"] != previous["workflow_run_id"]
    assert child["sessions"] == {} and child["tasks"] == [] and child["decisions"] == []
    assert child["activations"] == [] and child["retry_counts"] == {} and child["transitions"] == 0
    assert child["inherited_orchestrator_binding"] == selected
    assert selected["source_task_id"] == previous["sessions"]["orchestrator"]["task_id"]
    assert reuse.offer(store, parent, node, current["id"])["eligible_sessions"][0]["session_ref"].startswith("child-session:")


@pytest.mark.parametrize("state", ["running", "failed", "uncertain", "missing", "wrong_parent", "definition", "revision", "repo", "network", "receipt", "takeover", "cli", "model"])
def test_latest_source_fails_closed(store, source, monkeypatch, state):
    parent, node, old, child, current, record = source
    if state in {"running", "failed"}:
        child["status"] = state
    elif state == "uncertain":
        child["activations"][0]["tasks"][0]["status"] = "uncertain"
    elif state == "missing":
        child["sessions"] = {}
    elif state == "wrong_parent":
        child["parent_link"]["workflow_run_id"] = "another-parent"
    elif state == "definition":
        child["definition_hash"] = "changed"
    elif state == "revision":
        child["revision"] += 1
    elif state == "repo":
        child["repo_path"] += "/other"
    elif state == "network":
        record = replace(record, network=True)
    elif state == "receipt":
        record = replace(record, exit_code=None)
    elif state == "takeover":
        monkeypatch.setattr(tasks, "live_session_ids", lambda log_dir: {record.session_id})
    elif state == "cli":
        monkeypatch.setattr(w.backends, "is_installed", lambda backend: False)
    elif state == "model":
        record = replace(record, model="changed")
    write_run(store, child)
    tasks.write(store.root / "tasks", record)
    with pytest.raises(w.WorkflowError, match="Resume unavailable"):
        selection(store, source)
    assert not reuse.offer(store, parent, node, current["id"])["eligible_sessions"]


def test_never_skips_newer_incompatible_visit(store, source):
    parent, node, old, child, current, _ = source
    newer = copy.deepcopy(old)
    newer["id"] = "newer-failed"
    newer["invocation"]["child_workflow_run_id"] = "missing-newer-child"
    parent["activations"].insert(1, newer)
    assert not reuse.offer(store, parent, node, current["id"])["eligible_sessions"]
    parent["activations"] = [current]
    assert "No earlier" in reuse.offer(store, parent, node, current["id"])["unavailable_reason"]


def test_persisted_selection_refuses_source_change(store, source):
    parent, node, _, child, current, record = source
    current["invocation"]["child_session_selection"] = selection(store, source)
    child["sessions"]["orchestrator"]["session_id"] = "changed-session"
    record = replace(record, session_id="changed-session")
    tasks.write(store.root / "tasks", record)
    write_run(store, child)
    with pytest.raises(w.WorkflowError, match="source changed"):
        selection(store, source)


def test_agent_decides_both_modes_and_opaque_ref(store, source):
    parent, node, _, _, current, _ = source
    node["child_session_policy"] = "agent_decides"
    token = {**current["token"], "child_session_mode": "fresh", "child_session_reason": "Separate context"}
    assert reuse.select(store, parent, node, current, token)["selected_mode"] == "fresh"
    token.update(child_session_mode="resume", child_session_ref="invented")
    with pytest.raises(w.WorkflowError, match="issued eligible"):
        reuse.select(store, parent, node, current, token)
    token["child_session_ref"] = reuse.offer(store, parent, node, current["id"])["eligible_sessions"][0]["session_ref"]
    assert reuse.select(store, parent, node, current, token)["selected_mode"] == "resume"


class ReuseRegistry(TreeRegistry):
    def __init__(self, root, record, failure=None):
        super().__init__(root)
        self.resumes = []
        self.failure = failure
        previous = Task(record.task_id, {"session_id": record.session_id})
        previous.repo = Path(record.repo_path)
        previous.kwargs = {"backend": w.backends.get(record.backend), "freedom": "read_only", "network": record.network, "title": "child · Decision", "model": record.model, "reasoning_effort": record.reasoning_effort}
        self.tasks[record.task_id] = previous

    async def start(self, prompt, repo, **kwargs):
        task = await super().start(prompt, repo, **kwargs)
        record = tasks.read(self._log_dir, task.task_id)
        record = replace(record, exit_code=0, model=kwargs.get("model"), reasoning_effort=kwargs.get("reasoning_effort"))
        tasks.write(self._log_dir, record)
        return task

    async def resume(self, previous, prompt, **kwargs):
        self.resumes.append((previous.task_id, prompt))
        if self.failure:
            raise self.failure
        task = await super().resume(previous, prompt, **kwargs)
        task.result["session_id"] = previous.result["session_id"]
        record = tasks.read(self._log_dir, task.task_id)
        record = replace(record, session_id=task.result["session_id"], parent_task_id=previous.task_id)
        tasks.write(self._log_dir, record)
        return task


async def execute_new_child(store, source, registry):
    parent, node, _, _, current, _ = source
    selected = selection(store, source)
    current["invocation"]["child_session_selection"] = selected
    write_run(store, parent)
    child = inv.child_run_record(store, parent, current, node, current["invocation"])
    write_run(store, child)
    supervisor = w.WorkflowSupervisor(registry, store)
    await asyncio.wait_for(supervisor.execute(child["workflow_run_id"]), 15)
    return store.get_run(child["workflow_run_id"])


@pytest.mark.parametrize("policy", ["legacy", "optimized_v1"])
async def test_first_resume_bootstraps_new_scope_then_acknowledged_delta(store, source, policy):
    parent, _, _, _, _, record = source
    workflow_id = parent["definition"]["nodes"][1]["workflow_ref"]["workflow_id"]
    # Keep the pinned source and new scope on the same delivery policy.
    from polybridge.workflow_references import definition_sha256
    definition = parent["dependency_tree"]["workflows"][workflow_id]["definition"]
    definition["context_delivery"] = policy
    parent["dependency_tree"]["workflows"][workflow_id]["definition_sha256"] = definition_sha256(definition)
    previous = source[3]
    previous["definition"] = copy.deepcopy(definition)
    previous["definition_hash"] = definition_sha256(definition)
    from polybridge.workflow_references import subtree
    previous["dependency_tree"] = subtree(parent["dependency_tree"], workflow_id)
    write_run(store, previous)
    registry = ReuseRegistry(store.root, record)
    final = await execute_new_child(store, source, registry)
    assert final["status"] == "completed", final.get("attention_reason")
    assert registry.resumes[0][0] == record.task_id
    assert "NEW CHILD WORKFLOW INVOCATION" in registry.resumes[0][1]
    attempts = [t for a in final["activations"] if a["role"] == "orchestrator" for t in a["tasks"]]
    assert attempts[0]["session_mode"] == "resume"
    if policy == "optimized_v1":
        assert attempts[0]["context_delivery"]["delivery_mode"] == "bootstrap"
        assert any(t["context_delivery"]["delivery_mode"] == "delta" for t in attempts[1:])
    assert all(a.get("resume_task_id") != record.task_id for a in final["activations"] if a["role"] == "node")


@pytest.mark.parametrize("failure", [w.backends.UnsupportedCapability("resume unsupported"), RuntimeError("unknown launch")])
async def test_resume_refusal_or_ambiguity_never_starts_fresh(store, source, failure):
    registry = ReuseRegistry(store.root, source[-1], failure=failure)
    final = await execute_new_child(store, source, registry)
    assert final["status"] == "needs_attention"
    assert len(registry.resumes) == 1 and registry.dispatches == []
    attempts = [t for a in final["activations"] for t in a["tasks"]]
    assert attempts[0]["status"] == ("not_started" if isinstance(failure, w.backends.UnsupportedCapability) else "uncertain")
    assert final["sessions"] == {}


def test_actual_source_fallback_is_pinned(store, source):
    parent, node, _, child, current, record = source
    workflow_id = node["workflow_ref"]["workflow_id"]
    definition = parent["dependency_tree"]["workflows"][workflow_id]["definition"]
    fallback = {"backend": "codex", "model": "gpt-6-sol", "reasoning_effort": "medium"}
    definition["orchestrator"]["fallbacks"] = [fallback]
    from polybridge.workflow_references import definition_sha256, subtree
    parent["dependency_tree"]["workflows"][workflow_id]["definition_sha256"] = definition_sha256(definition)
    child["definition"] = copy.deepcopy(definition)
    child["definition_hash"] = definition_sha256(definition)
    child["dependency_tree"] = subtree(parent["dependency_tree"], workflow_id)
    child["sessions"]["orchestrator"]["candidate"] = "orchestrator:" + w._candidate_key(fallback)
    child["activations"][0]["tasks"][0]["candidate"] = fallback
    tasks.write(store.root / "tasks", replace(record, model=fallback["model"], reasoning_effort=fallback["reasoning_effort"]))
    write_run(store, child)
    assert reuse.select(store, parent, node, current, current["token"])["candidate"] == fallback


@pytest.mark.parametrize("phase", ["before_publication", "after_publication"])
async def test_publication_recovery_preserves_one_selection_and_child(store, source, monkeypatch, phase):
    parent, node, _, _, current, record = source
    registry = ReuseRegistry(store.root, record)
    supervisor = w.WorkflowSupervisor(registry, store)
    supervisor.run_id = parent["workflow_run_id"]
    supervisor.tree.root_run_id = parent["workflow_run_id"]
    publish = inv.write_child_run
    def crash(storage, child):
        if phase == "after_publication":
            publish(storage, child)
        raise OSError("synthetic crash")
    with monkeypatch.context() as patch:
        patch.setattr(inv, "write_child_run", crash)
        with pytest.raises(OSError, match="synthetic crash"):
            await inv.run_child(supervisor, node, current["token"])
    reserved = store.get_run(parent["workflow_run_id"])["activations"][-1]["invocation"]
    assert reserved["child_session_selection"]["selected_mode"] == "resume"
    assert registry.resumes == []
    await asyncio.wait_for(inv.run_child(supervisor, node, current["token"]), 15)
    final = store.get_run(parent["workflow_run_id"])["activations"][-1]["invocation"]
    assert final["child_workflow_run_id"] == reserved["child_workflow_run_id"]
    assert final["child_session_selection"] == reserved["child_session_selection"]
    assert registry.resumes[0][0] == record.task_id
    assert store.get_run(final["child_workflow_run_id"])["status"] == "completed"


async def test_adoption_refuses_changed_published_binding(store, source):
    parent, node, _, _, current, _ = source
    selected = selection(store, source)
    current["invocation"]["child_session_selection"] = selected
    write_run(store, parent)
    child = inv.child_run_record(store, parent, current, node, current["invocation"])
    child["inherited_orchestrator_binding"]["source_task_id"] = "another-task"
    write_run(store, child)
    registry = ReuseRegistry(store.root, source[-1])
    supervisor = w.WorkflowSupervisor(registry, store)
    supervisor.run_id = parent["workflow_run_id"]
    supervisor.tree.root_run_id = parent["workflow_run_id"]
    await inv.run_child(supervisor, node, current["token"])
    assert registry.resumes == [] and registry.dispatches == []
    assert store.get_run(parent["workflow_run_id"])["status"] == "needs_attention"


def test_fixed_resume_revalidates_checkpoint_issued_reference(store, source):
    parent, node, _, _, current, _ = source
    token = {**current["token"], "child_session_ref": "expired-issued-reference"}
    with pytest.raises(w.WorkflowError, match="issued eligible"):
        reuse.select(store, parent, node, current, token)


@pytest.mark.parametrize("error", ["unknown", "busy"])
async def test_missing_or_busy_session_never_launches_fresh(store, source, error):
    from polybridge.tasks import SessionBusyError, SessionUnknownError
    exc = SessionUnknownError("expired") if error == "unknown" else SessionBusyError("takeover held")
    registry = ReuseRegistry(store.root, source[-1], failure=exc)
    final = await execute_new_child(store, source, registry)
    assert final["status"] == "needs_attention"
    assert len(registry.resumes) == 1 and registry.dispatches == []
    parent = store.get_run(source[0]["workflow_run_id"])
    assert parent["activations"][-1]["invocation"]["child_session_refusal"]


def test_cli_resume_preflight_refuses_unsupported_shape(store, source, monkeypatch):
    backend = w.backends.get("codex")
    monkeypatch.setattr(type(backend), "build_resume_argv", lambda *a, **k: (_ for _ in ()).throw(w.backends.UnsupportedCapability("no certified resume")))
    offered = reuse.offer(store, source[0], source[1], source[4]["id"])
    assert not offered["eligible_sessions"] and "no certified resume" in offered["unavailable_reason"]


@pytest.mark.parametrize("field", ["model", "reasoning_effort"])
def test_unknown_ambient_configuration_is_not_compatible(store, source, field):
    parent, node, _, child, current, record = source
    workflow_id = node["workflow_ref"]["workflow_id"]
    definition = parent["dependency_tree"]["workflows"][workflow_id]["definition"]
    definition["orchestrator"].pop(field)
    from polybridge.workflow_references import definition_sha256, subtree
    parent["dependency_tree"]["workflows"][workflow_id]["definition_sha256"] = definition_sha256(definition)
    child["definition"] = copy.deepcopy(definition)
    child["definition_hash"] = definition_sha256(definition)
    child["dependency_tree"] = subtree(parent["dependency_tree"], workflow_id)
    child["sessions"]["orchestrator"]["candidate"] = "orchestrator:" + w._candidate_key(definition["orchestrator"])
    child["activations"][0]["tasks"][0]["candidate"] = copy.deepcopy(definition["orchestrator"])
    tasks.write(store.root / "tasks", replace(record, **{field: None}))
    write_run(store, child)
    offered = reuse.offer(store, parent, node, current["id"])
    assert offered["eligible_sessions"] == []
    assert "unknown" in offered["unavailable_reason"] and "explicit saved" in offered["unavailable_reason"]


def test_unpinnable_model_backend_refuses_reuse(store, source, monkeypatch):
    backend = w.backends.get("codex")
    monkeypatch.setattr(backend, "capabilities", backend.capabilities._replace(supports_model_selection=False))
    offered = reuse.offer(store, source[0], source[1], source[4]["id"])
    assert not offered["eligible_sessions"] and "cannot pin" in offered["unavailable_reason"]


async def test_resume_with_changed_conversation_identity_cannot_advance(store, source):
    class ChangedSession(ReuseRegistry):
        async def resume(self, previous, prompt, **kwargs):
            task = await super().resume(previous, prompt, **kwargs)
            task.result["session_id"] = "unexpected-fresh-conversation"
            return task
    registry = ChangedSession(store.root, source[-1])
    final = await execute_new_child(store, source, registry)
    assert final["status"] == "needs_attention"
    assert final["sessions"] == {}
    assert not any(dispatch["label"] == "work" for dispatch in registry.dispatches)
    assert "conversation identity" in final["child_session_refusal"]


def test_harness_that_may_start_fresh_cannot_launch_child_resume(store, source, monkeypatch):
    backend = w.backends.get("codex")
    monkeypatch.setattr(backend, "capabilities", backend.capabilities._replace(resume_may_start_fresh=True))
    offered = reuse.offer(store, source[0], source[1], source[4]["id"])
    assert not offered["eligible_sessions"] and "may start a Fresh" in offered["unavailable_reason"]
    with pytest.raises(w.WorkflowError, match="strict Child Resume is unsupported"):
        selection(store, source)


@pytest.mark.parametrize("undisclosed", [False, True])
def test_completed_external_resume_invalidates_offered_and_saved_source(store, source, undisclosed):
    parent, node, _, _, current, record = source
    selected = selection(store, source)
    current["invocation"]["child_session_selection"] = selected
    tasks.write(store.root / "tasks", replace(record, task_id="external-successor", parent_task_id=record.task_id, session_id=None if undisclosed else record.session_id))
    assert record.session_id not in tasks.live_session_ids(store.root / "tasks")
    offered = reuse.offer(store, parent, node, current["id"])
    assert offered["eligible_sessions"] == [] and "successor" in offered["unavailable_reason"]
    with pytest.raises(w.WorkflowError, match="successor"):
        selection(store, source)
    # Recovery retains the selected source instead of recomputing Fresh.
    assert current["invocation"]["child_session_selection"] == selected


async def test_successor_between_workflow_validation_and_lock_never_spawns(store, source, monkeypatch):
    from contextlib import asynccontextmanager
    from polybridge import control
    from polybridge.tasks import TaskRegistry
    record = source[-1]
    actual = TaskRegistry(log_dir=store.root / "tasks")
    async def no_caller():
        return None
    monkeypatch.setattr(actual, "_mutation_caller", no_caller)
    spawns = []
    async def no_spawn(*args, **kwargs):
        spawns.append(kwargs)
        raise AssertionError("Strict child resume must refuse before spawn")
    monkeypatch.setattr(actual, "_spawn", no_spawn)
    original_lock = control.session_lock
    @asynccontextmanager
    async def inject(log_dir, session_id, **kwargs):
        async with original_lock(log_dir, session_id, **kwargs):
            tasks.write(log_dir, replace(record, task_id="external-successor", parent_task_id=record.task_id, session_id=None))
            yield
    monkeypatch.setattr(control, "session_lock", inject)
    class LockedRegistry(ReuseRegistry):
        async def resume(self, previous, prompt, **kwargs):
            self.resumes.append((previous.task_id, prompt))
            assert kwargs["require_unchanged_session"] is True
            return await actual.resume_record(record, prompt, **kwargs)
    registry = LockedRegistry(store.root, record)
    final = await execute_new_child(store, source, registry)
    assert final["status"] == "needs_attention"
    assert spawns == [] and registry.dispatches == [] and final["sessions"] == {}
    assert len(registry.resumes) == 1
    attempt = next(t for a in final["activations"] for t in a["tasks"])
    assert attempt["status"] == "not_started" and attempt["session_mode"] == "resume"
    assert "successor" in final["child_session_refusal"]
