"""Native execution authority and backwards-compatible workflow scheduling."""
import asyncio
import copy
import json
from dataclasses import replace

import pytest

from polybridge import store, workflows as w
from polybridge.backends.claude import ClaudeBackend, UnsafeInvocationError
from polybridge.backends.claude_native import ClaudeNativeAdapter, NATIVE_ARGS
from polybridge.workflow_invocation import WorkflowTree
from polybridge.workflow_native import POLICY, dispatch_native, scheduling_policy
from test_workflow_delegation import Registry, Task, graph

MODEL = "claude-sonnet-4-6"


def native_graph():
    g = graph("review")
    g["orchestrator"] = {"backend": "claude", "model": MODEL}
    g["nodes"][1].update(agent={"backend": "claude", "model": MODEL}, execution_mode="prefer_subagent", session_mode="fresh")
    return g


def native_events(nonce, assignment, session):
    def event(kind, **fields):
        return {"type": kind, "session_id": session, **fields}
    return [
        event("system", subtype="init", claude_code_version="2.1.290", permissionMode="plan"),
        event("assistant", message={"content": [{"type": "tool_use", "id": "tool-child", "name": "Agent", "input": {"description": nonce, "prompt": assignment, "subagent_type": "pb-node", "run_in_background": False}}]}),
        event("system", subtype="task_started", task_id="native-child", tool_use_id="tool-child", description=nonce, prompt=assignment, subagent_type="pb-node", is_backgrounded=False, task_type="local_agent"),
        event("assistant", parent_tool_use_id="tool-child", message={"content": [{"type": "text", "text": "Child progress"}]}),
        event("user", message={"content": [{"type": "tool_result", "tool_use_id": "tool-child"}]}, tool_use_result={"status": "completed", "agentId": "native-child", "agentType": "pb-node", "prompt": assignment, "resolvedModel": MODEL, "content": [{"type": "text", "text": json.dumps({"status": "succeeded", "result": {"summary": "Native reviewed", "verdict": "approved"}, "evidence": ["observed"]})}]}),
    ]


class NativeRegistry(Registry):
    def __init__(self, root, *, terminal=True, forged=False):
        super().__init__(root)
        self.outputs = {"work": {"status": "succeeded", "result": {"summary": "Reviewed", "verdict": "approved"}, "evidence": []}}
        self.terminal = terminal
        self.forged = forged
        self.native_calls = []

    async def start(self, prompt, repo, **kwargs):
        task = await super().start(prompt, repo, **kwargs)
        record = store.read(self._log_dir, task.task_id)
        store.write(self._log_dir, replace(record, model=MODEL, max_turns=100))
        return task

    async def resume(self, previous, prompt, **kwargs):
        if not kwargs.get("native_subagent"):
            task = await super().resume(previous, prompt, **kwargs)
            previous_record = store.read(self._log_dir, previous.task_id)
            record = store.read(self._log_dir, task.task_id)
            task.result["session_id"] = previous_record.session_id
            store.write(self._log_dir, replace(record, session_id=previous_record.session_id))
            return task
        self.native_calls.append(kwargs)
        arguments = json.loads(prompt.split("Agent arguments:\n", 1)[1])
        record = store.read(self._log_dir, previous.task_id)
        events = native_events(arguments["description"], arguments["prompt"], record.session_id)
        if self.forged:
            events[2]["description"] = "wrong-reservation"
        if not self.terminal:
            events = events[:-1]
        for event in events:
            self._workflow_native_observers[kwargs["task_id"]](event)
        task = Task(kwargs["task_id"], {"summary": "Parent transport finished", "backend": "claude", "session_id": record.session_id})
        task.kwargs, task.repo = previous.kwargs, previous.repo
        self.tasks[task.task_id] = task
        store.write(self._log_dir, replace(record, task_id=task.task_id))
        return task


@pytest.fixture
def native_setup(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda b: True)
    monkeypatch.setattr(w.backends, "version", lambda b: "2.1.290 (Claude Code)")
    storage = w.WorkflowStore(tmp_path)
    registry = NativeRegistry(tmp_path)
    return storage, registry


def test_execution_preference_defaults_and_validation():
    assert w.validate_definition(graph())["nodes"][1]["execution_mode"] == "headless"
    g = native_graph()
    g["nodes"][1]["execution_mode"] = "required_subagent"
    with pytest.raises(w.WorkflowError, match="execution mode"):
        w.validate_definition(g)


def test_pinned_tree_policy_is_frozen_before_first_reservation(native_setup, tmp_path):
    storage, _ = native_setup
    definition = w.validate_definition(graph())
    tree = {"workflows": {"child": {"definition": native_graph()}}}
    run = storage.create_run(definition, "goal", tmp_path, dependency_tree=tree)
    assert run["scheduling_policy"] == POLICY
    assert scheduling_policy(definition) == "legacy"
    assert storage.get_run(run["workflow_run_id"])["scheduling_policy"] == POLICY


async def test_native_node_uses_owner_transport_and_no_child_process_record(native_setup, tmp_path):
    storage, registry = native_setup
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason")
    worker = next(a for a in observed["activations"] if a["role"] == "node")
    attempt = worker["tasks"][0]
    assert attempt["execution_kind"] == "native_subagent"
    assert attempt["native_child_id"] == "native-child"
    assert attempt["result"]["summary"].startswith('{"status": "succeeded"')
    assert attempt["harness_metadata"]["observed"]["model"] == MODEL
    assert store.read(registry._log_dir, attempt["task_id"]) is None
    assert store.read(registry._log_dir, attempt["transport_task_id"]) is not None
    assert registry.native_calls[0]["max_turns"] == 100
    assert worker["can_cancel_child"] is False
    assert worker["can_resume_child"] is False
    assert worker["execution_kind"] == "native_subagent"


@pytest.mark.parametrize("terminal,forged", [(False, False), (True, True)])
async def test_unproven_native_never_falls_back_or_completes(native_setup, tmp_path, terminal, forged):
    storage, _ = native_setup
    registry = NativeRegistry(tmp_path, terminal=terminal, forged=forged)
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    worker = next(a for a in observed["activations"] if a["role"] == "node")
    assert worker["tasks"][0]["status"] == "uncertain"
    assert len(registry.native_calls) == 1
    assert all("Context:\n" in prompt for prompt, _ in registry.calls)
    storage.reconcile_run(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["activations"][1]["tasks"][0]["status"] == "uncertain"


async def test_unsupported_model_falls_back_before_child_launch(native_setup, tmp_path):
    storage, registry = native_setup
    definition = native_graph()
    definition["nodes"][1]["agent"]["model"] = "other-model"
    run = storage.create_run(w.validate_definition(definition), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    worker = next(a for a in observed["activations"] if a["role"] == "node")
    assert not registry.native_calls
    assert worker["execution_kind"] == "headless"
    assert "inherit" in worker["execution_fallback_reason"]
    assert worker["tasks"][0]["execution_fallback_reason"]
    assert observed["scheduling_policy"] == POLICY


async def test_native_worker_and_control_accounting_at_one_and_held_release(native_setup):
    storage, _ = native_setup
    tree = WorkflowTree(storage, permits=1, scheduling_policy=POLICY)
    await tree.acquire_slot(lambda: True)
    await tree.acquire_slot(lambda: True, control=True)
    assert tree.slots.free == tree.control_slots.free == 0
    tree.hold_slot("r", "child")
    tree.hold_slot("r", "parent", control=True)
    tree.release_held("r")
    assert tree.slots.free == tree.control_slots.free == 1
    legacy = WorkflowTree(storage, permits=1)
    assert legacy.control_slots is None


def test_adapter_rejects_stale_duplicate_and_wrong_result_identity():
    adapter = ClaudeNativeAdapter()
    state = {"assignment": "exact", "owner_session_id": "session", "expected_model": MODEL}
    events = native_events("nonce", "exact", "session")
    for event in events[:3]:
        adapter.observe(event, "nonce", state)
    wrong = copy.deepcopy(events[-1])
    wrong["tool_use_result"]["agentId"] = "foreign"
    with pytest.raises(ValueError, match="did not match"):
        adapter.observe(wrong, "nonce", state)
    with pytest.raises(ValueError, match="foreign parent"):
        adapter.observe({"session_id": "stale"}, "nonce", state)


def test_native_invocation_validator_is_exact(tmp_path):
    backend = ClaudeBackend()
    original = backend.build_resume_argv("assignment", repo=tmp_path, freedom="read_only", session_id="session", model=MODEL, max_turns=100, reasoning_effort=None)
    invocation = ClaudeNativeAdapter().configure(original)
    backend.assert_safe(invocation, "read_only")
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(invocation, native_subagent=False), "read_only")
    altered = list(invocation.argv)
    altered[altered.index("--settings") + 1] = '{"permissions":{"defaultMode":"bypassPermissions"}}'
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(invocation, argv=altered), "read_only")


def test_new_save_prefers_native_and_legacy_load_edit_preserves_headless(native_setup):
    storage, _ = native_setup
    new = storage.save("new", graph())
    assert new["nodes"][1]["execution_mode"] == "prefer_subagent"
    legacy = w.validate_definition(graph())
    legacy["name"] = "old"
    legacy["revision"] = 1
    legacy["nodes"][1].pop("execution_mode")
    (storage.definitions / "old.json").write_text(json.dumps(legacy))
    loaded = storage.get("old")
    assert loaded["nodes"][1]["execution_mode"] == "headless"
    assert next(d for d in storage.list() if d["name"] == "old")["nodes"][1]["execution_mode"] == "headless"
    updated = storage.save("old", legacy, expected_revision=1)
    assert updated["nodes"][1]["execution_mode"] == "headless"
    explicit = graph()
    explicit["nodes"][1]["execution_mode"] = "headless"
    assert storage.save("explicit", explicit)["nodes"][1]["execution_mode"] == "headless"


@pytest.mark.parametrize("owner_alive", [False, True])
def test_uncertain_child_holds_checkout_after_owner_dies(native_setup, tmp_path, monkeypatch, owner_alive):
    storage, _ = native_setup
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    def unresolved(r):
        r.update(status="failed", supervisor_pid=None, supervisor_identity=None)
        r["activations"] = [{"id": "execution", "node_id": "work", "role": "node", "status": "uncertain", "tasks": [{"task_id": "child-execution", "execution_kind": "native_subagent", "status": "uncertain", "freedom": "read_only", "dispatch_stage": "launch_requested", "transport_task_id": "transport"}]}]
    storage.update_run(run["workflow_run_id"], unresolved, "crash_fixture")
    storage.list_run_page()
    monkeypatch.setattr(w, "_supervisor_present", lambda task: owner_alive)
    lease = w.CheckoutLease(storage, str(tmp_path), write=True)
    assert lease._orphan_owner()["task_id"] == "child-execution"
    assert "child-execution" in storage.pinned_tasks()
    if not owner_alive:
        with pytest.raises(w.WorkflowError, match="child reconciliation"):
            storage.abandon_dispatch(run["workflow_run_id"], "execution", "child-execution", "Parent dead", True)
    storage.update_run(run["workflow_run_id"], lambda r: r["activations"][0]["tasks"][0].update(status="completed", native_terminal=True), "authoritative_settlement_fixture")
    assert lease._orphan_owner() is None


def test_native_independent_takeover_is_refused_even_after_run_completed(native_setup, tmp_path):
    from polybridge import control
    from polybridge.workflow_hooks import refuse_takeover
    storage, registry = native_setup
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="completed", activations=[{"id": "execution", "node_id": "work", "role": "node", "status": "completed", "tasks": [{"task_id": "child-execution", "execution_kind": "native_subagent", "status": "completed"}]}]), "settled_fixture")
    with pytest.raises(control.TakeoverRefused, match="cannot be taken over independently"):
        refuse_takeover(registry._log_dir, "child-execution")


async def test_live_native_activity_has_child_origin_without_process_record(native_setup, tmp_path):
    from polybridge.workflow_inspection import inspect_request
    from polybridge.events import EventLog, events_path
    storage, registry = native_setup
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    observed = storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", activations=[{"id": "execution", "node_id": "work", "role": "node", "status": "running", "tasks": [{"task_id": "child-execution", "execution_kind": "native_subagent", "status": "running"}]}]), "live_fixture")
    registry._log_dir.mkdir(exist_ok=True)
    log = EventLog(events_path(registry._log_dir, "child-execution"), "child-execution")
    log.write("assistant_text", {"text": "Reading source", "native_child_id": "native-actual"})
    log.close()
    page = inspect_request(observed, storage.root, {"execution_id": "execution", "view": "activity"})
    assert page["events"][0]["native_child_id"] == "native-actual"
    assert store.read(registry._log_dir, "child-execution") is None


def test_native_cap_exhaustion_cannot_complete_even_with_tool_completed():
    native = ClaudeNativeAdapter()
    state = {"assignment": "exact", "owner_session_id": "session", "expected_model": MODEL}
    events = native_events("nonce", "exact", "session")
    events[-1]["tool_use_result"]["harnessNoteCount"] = 1
    for event in events[:-1]:
        native.observe(event, "nonce", state)
    outcome = native.observe(events[-1], "nonce", state)
    assert next(u for u in outcome if u["native_update"] == "settled")["status"] == "failed"


async def test_authoritative_observer_failure_does_not_stop_parent_stdout_drain(tmp_path):
    from types import SimpleNamespace
    from test_cancel import make_task
    from polybridge.tasks import _drain_stdout
    task = make_task(tmp_path, "transport")
    reader = asyncio.StreamReader()
    reader.feed_data((json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "First"}]}}) + "\n" + json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "Second"}]}}) + "\n").encode())
    reader.feed_eof()
    task.proc = SimpleNamespace(stdout=reader)
    def fail(_):
        raise OSError("authoritative storage failed")
    registry = SimpleNamespace(_workflow_native_observers={"transport": fail})
    await _drain_stdout(task, registry)
    assert registry._workflow_native_failures["transport"] == "authoritative storage failed"
    assert "transport" not in registry._workflow_native_observers
    assert task.drain_failed is False
    assert len(task.tail) == 2
    assert "Second" in task.tail[-1]


async def test_parent_permission_evidence_preserved_on_native_child(native_setup, tmp_path, monkeypatch):
    from polybridge.workflow_delegation import caller_decision_block
    storage, _ = native_setup
    original_events = native_events
    denial = {"tool_name": "Bash", "tool_use_id": "refused", "reason": "Permission to use Bash denied"}
    def blocked_events(nonce, assignment, session):
        events = original_events(nonce, assignment, session)
        events[-1]["tool_use_result"]["content"][0]["text"] = json.dumps({"status": "blocked", "result": {"blocker_category": "missing_context", "reason": "Need permission"}, "evidence": []})
        events.append({"type": "result", "session_id": session, "permission_denials": [denial]})
        return events
    monkeypatch.setattr(__import__(__name__), "native_events", blocked_events)
    registry = NativeRegistry(tmp_path)
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    execution = next(a for a in observed["activations"] if a["role"] == "node")
    assert execution["tasks"][0]["result"]["permission_denials"] == [denial]
    assert caller_decision_block(execution) is False
    assert len(registry.native_calls) == 1


@pytest.mark.parametrize("root_cancel", [False, True])
async def test_native_timeout_and_root_cancel_cannot_invent_child_settlement(native_setup, tmp_path, root_cancel):
    storage, _ = native_setup
    cancellations = []
    class HangingRegistry(NativeRegistry):
        async def resume(self, previous, prompt, **kwargs):
            task = await super().resume(previous, prompt, **kwargs)
            if kwargs.get("native_subagent"):
                task.done.clear()
                if root_cancel:
                    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="cancelling"), "root_cancel_fixture")
            return task
        async def cancel_cascade(self, task_id, **kwargs):
            cancellations.append(task_id)
            task = self.tasks.get(task_id)
            if task:
                task.result.update(status="cancelled")
                task.done.set()
            return {}
    registry = HangingRegistry(tmp_path, terminal=False)
    definition = native_graph()
    definition["nodes"][1]["timeout_seconds"] = 1
    run = storage.create_run(w.validate_definition(definition), "Review", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    execution = next(a for a in observed["activations"] if a["role"] == "node")
    attempt = execution["tasks"][0]
    assert attempt["status"] == "uncertain"
    assert attempt["transport_task_id"] in cancellations
    assert attempt["task_id"] not in cancellations
    assert len(registry.native_calls) == 1
    assert observed["status"] == "needs_attention"
    assert attempt["task_id"] in storage.pinned_tasks()


def test_native_tool_activity_uses_monitor_contract():
    adapter = ClaudeNativeAdapter()
    state = {"assignment": "exact", "owner_session_id": "session", "expected_model": MODEL}
    for event in native_events("nonce", "exact", "session")[:3]:
        adapter.observe(event, "nonce", state)
    calls = adapter.observe({"type": "assistant", "session_id": "session", "parent_tool_use_id": "tool-child", "message": {"content": [{"type": "tool_use", "id": "read", "name": "Read", "input": {"file_path": "/tmp/source.py"}}]}}, "nonce", state)
    call = calls[0]
    assert call["event_kind"] == "tool_call"
    assert call["category"] == "read" and call["path"] == "/tmp/source.py"
    assert '"file_path"' in call["input_preview"]
    assert "input" not in call
    results = adapter.observe({"type": "user", "session_id": "session", "parent_tool_use_id": "tool-child", "message": {"content": [{"type": "tool_result", "tool_use_id": "read", "content": "Permission to use Read denied", "is_error": True}]}}, "nonce", state)
    result = results[0]
    assert result["output_tail"] == "Permission to use Read denied"
    assert result["ok"] is False


def test_native_permission_evidence_union_preserves_child_and_parent():
    from polybridge.workflow_native import merge_denials
    child = {"tool_name": "Read", "reason": "Child denied"}
    parent = {"tool_name": "Bash", "reason": "Parent denied"}
    assert merge_denials([child], [parent, child]) == [child, parent]


async def test_duplicate_native_child_identity_across_same_owner_session_is_fenced(native_setup, tmp_path):
    storage, registry = native_setup
    definition = native_graph()
    second = copy.deepcopy(definition["nodes"][1])
    second["id"] = "second"
    definition["nodes"].insert(2, second)
    definition["connections"][1]["target"] = "second"
    definition["connections"].append({"id": "second-finish", "source": "second", "target": "end"})
    run = storage.create_run(w.validate_definition(definition), "Review twice", tmp_path)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "needs_attention"
    workers = [a for a in observed["activations"] if a["role"] == "node"]
    assert workers[0]["tasks"][0]["status"] == "completed"
    assert workers[1]["tasks"][0]["status"] == "uncertain"
    assert "reused across executions" in workers[1]["tasks"][0]["error"]
    assert len(registry.native_calls) == 2

@pytest.mark.parametrize("persisted", [False, True])
@pytest.mark.parametrize("refusal", ["busy", "unknown", "repo", "positive", "ambiguous"])
async def test_native_resume_refusal_releases_only_proven_no_spawn(native_setup, tmp_path, monkeypatch, persisted, refusal):
    from polybridge.tasks import SessionBusyError, SessionUnknownError, RepoUnavailableError
    storage, _ = native_setup
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    errors = {"busy": SessionBusyError, "unknown": SessionUnknownError, "repo": RepoUnavailableError, "positive": OSError, "ambiguous": RuntimeError}
    error = errors[refusal]("resume refused")
    if refusal == "positive":
        error.polybridge_not_started = True

    class RefusingRegistry(NativeRegistry):
        refuse = True
        paths = []
        def get(self, task_id):
            return None if persisted else super().get(task_id)
        async def resume_record(self, record, prompt, **kwargs):
            return await self.resume(self.tasks[record.task_id], prompt, **kwargs)
        async def resume(self, previous, prompt, **kwargs):
            if kwargs.get("native_subagent") and self.refuse:
                self.paths.append("persisted" if persisted else "live")
                raise error
            return await super().resume(previous, prompt, **kwargs)

    registry = RefusingRegistry(tmp_path)
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    supervisor = w.WorkflowSupervisor(registry, storage)
    await asyncio.wait_for(supervisor.execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    node = next(a for a in observed["activations"] if a["role"] == "node")
    transport = next(a for a in observed["activations"] if a["role"] == "native_control")
    child = node["tasks"][0]
    assert registry.paths == ["persisted" if persisted else "live"]
    assert observed["status"] == "needs_attention"
    assert store.read(registry._log_dir, child["task_id"]) is None
    assert store.read(registry._log_dir, transport["id"]) is None
    if refusal == "ambiguous":
        assert child["status"] == "uncertain"
        assert child["task_id"] in storage.pinned_tasks()
        assert observed["settling"]
        assert w.CheckoutLease(storage, str(tmp_path), True)._orphan_owner() is not None
        return
    assert node["status"] == child["status"] == "not_started"
    assert transport["status"] == transport["tasks"][0]["status"] == "not_started"
    assert not observed["settling"]
    assert not supervisor.tree.held
    assert supervisor.tree.slots.free == observed["definition"]["max_parallel"]
    assert supervisor.tree.control_slots.free == 1
    assert w.CheckoutLease(storage, str(tmp_path), True)._orphan_owner() is None
    registry.refuse = False
    storage.control(run["workflow_run_id"], "resume")
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"
    assert len(registry.native_calls) == 1


async def test_native_transport_direct_cancel_and_takeover_are_refused(native_setup, tmp_path, monkeypatch):
    from polybridge.tasks import TaskRegistry
    from polybridge.workflow_hooks import refuse_takeover
    from polybridge.control import TakeoverRefused
    storage, _ = native_setup
    run = storage.create_run(w.validate_definition(native_graph()), "Review", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", activations=[{"id": "transport", "node_id": "orchestrator", "role": "native_control", "status": "running", "tasks": [{"task_id": "transport", "status": "running"}]}]), "transport_fixture")
    registry = TaskRegistry(log_dir=tmp_path / "tasks", open_monitor=False)
    calls = []
    async def cancel(task_id):
        calls.append(task_id)
        return {"cancelled": True}
    monkeypatch.setattr(registry, "_cancel_cascade", cancel)
    with pytest.raises(w.WorkflowError, match="root workflow"):
        await registry.cancel_cascade("transport")
    assert calls == []
    with pytest.raises(TakeoverRefused, match="Internal native transports"):
        refuse_takeover(tmp_path / "tasks", "transport")
    assert await registry.cancel_cascade("transport", workflow_control=True) == {"cancelled": True}
    assert calls == ["transport"]
