"""Run workflow node execution: child and current orchestrator modes, tree budget."""
import asyncio
import copy
import json

import pytest

from polybridge import workflow_delegation as d
from polybridge import workflow_references as refs
from polybridge import workflows as w

from test_workflow_delegation import Registry, Task, storage


def agent_node(node_id: str, instructions: str = "Do the work") -> dict:
    return {"id": node_id, "type": "agent", "role": "task", "instructions": instructions, "agent": {"backend": "codex"}}


def child_graph(name: str = "child", *, parallel: bool = False, work_count: int = 1) -> dict:
    nodes = [{"id": "start", "type": "start"}]
    connections = []
    if parallel:
        nodes.append({"id": "split", "type": "parallel_start", "parallel_group_id": "g"})
        nodes.append({"id": "join", "type": "parallel_end", "parallel_group_id": "g"})
        for index in range(work_count):
            nodes.append(agent_node(f"work{index + 1}"))
            connections.append({"id": f"b{index}", "source": "split", "target": f"work{index + 1}"})
            connections.append({"id": f"j{index}", "source": f"work{index + 1}", "target": "join"})
        connections.append({"id": "begin", "source": "start", "target": "split"})
        connections.append({"id": "done", "source": "join", "target": "end"})
    else:
        nodes.append(agent_node("work"))
        connections = [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]
    nodes.append({"id": "end", "type": "end"})
    return {"name": name, "routing_mode": "explicit", "orchestrator": {"backend": "codex"}, "nodes": nodes, "connections": connections}


def parent_graph(workflow_id: str, *, name: str = "parent", mode: str = "child", prep: bool = False, branches: int = 1, max_parallel: int = 4, timeout_seconds: int | None = None) -> dict:
    nodes: list[dict] = [{"id": "start", "type": "start"}]
    connections: list[dict] = []
    if prep:
        nodes.append(agent_node("prep", "Prepare the input"))
        connections.append({"id": "to-prep", "source": "start", "target": "prep"})
        last = "prep"
    else:
        last = "start"
    if branches > 1:
        nodes.append({"id": "split", "type": "parallel_start", "parallel_group_id": "p"})
        nodes.append({"id": "join", "type": "parallel_end", "parallel_group_id": "p"})
        connections.append({"id": "to-split", "source": last, "target": "split"})
        for index in range(branches):
            call = {"id": f"call{index + 1}", "type": "workflow", "workflow_ref": {"workflow_id": workflow_id}, "orchestrator_mode": mode, "instructions": "Delegate"}
            if timeout_seconds is not None:
                call["timeout_seconds"] = timeout_seconds
            nodes.append(call)
            connections.append({"id": f"c{index}", "source": "split", "target": f"call{index + 1}"})
            connections.append({"id": f"cj{index}", "source": f"call{index + 1}", "target": "join"})
        connections.append({"id": "done", "source": "join", "target": "end"})
    else:
        call = {"id": "call", "type": "workflow", "workflow_ref": {"workflow_id": workflow_id}, "orchestrator_mode": mode, "instructions": "Delegate"}
        if timeout_seconds is not None:
            call["timeout_seconds"] = timeout_seconds
        nodes.append(call)
        connections.append({"id": "to-call", "source": last, "target": "call"})
        connections.append({"id": "done", "source": "call", "target": "end"})
    nodes.append({"id": "end", "type": "end"})
    return {"name": name, "routing_mode": "explicit", "orchestrator": {"backend": "codex"}, "max_parallel": max_parallel, "nodes": nodes, "connections": connections}


def assignment_for(choice):
    entry = {"continuation_id": choice["continuation_id"]}
    if choice["requires_prompt"]:
        entry["prompt"] = f"Focused assignment for {choice['node_id']}"
        if not choice.get("workflow"):
            entry["session_mode"] = "fresh"
    if "branch_continuations" in choice:
        entry["branch_assignments"] = [assignment_for(branch) for branch in choice["branch_continuations"]]
    return entry


def workflow_policy(context, registry):
    """Assign prompts; workflow targets take no session_mode; retries are never auto-selected."""
    choices = [c for c in context["valid_continuations"] if c["kind"] != "retry_execution"]
    if not choices:
        return {"decision_id": context["decision_id"], "action": "complete", "reason": "All branches settled", "next": []}
    return {"decision_id": context["decision_id"], "action": "continue", "reason": "Evidence supports continuation", "next": [assignment_for(choice) for choice in choices]}


class TreeRegistry(Registry):
    """Fake registry that records every dispatch and can hold or fail one mid-flight."""

    def __init__(self, root, policy=None, outputs=None):
        super().__init__(root, policy or workflow_policy, outputs)
        self.dispatches: list[dict] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self.gates: dict[str, asyncio.Event] = {}
        self.reached: dict[str, bool] = {}
        self.fail_after_gate: set[str] = set()
        self.occurrences: dict[str, int] = {}
        self.hold_when: dict[str, int] = {}
        self.fail_when: dict[str, int] = {}

    async def start(self, prompt, repo, **kwargs):
        title = kwargs.get("title", "")
        label = title.split(" · ")[-1]
        run_name = title.split(" · ")[0]
        occurrence = self.occurrences[title] = self.occurrences.get(title, 0) + 1
        if self.hold_when.get(title) == occurrence:
            gate = self.gates.setdefault(title, asyncio.Event())
            self.reached[title] = True
            await gate.wait()
            if self.fail_when.get(title) == occurrence:
                raise RuntimeError("simulated in-flight harness failure")
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            task = await super().start(prompt, repo, **kwargs)
        finally:
            self.in_flight -= 1
        self.dispatches.append({"title": title, "label": label, "run": run_name, "occurrence": occurrence, "kwargs": copy.deepcopy({k: v for k, v in kwargs.items() if k in ("freedom", "network", "task_id", "max_turns")}), "session_id": task.result.get("session_id"), "prompt": prompt})
        return task


class SessionRegistry(TreeRegistry):
    """Keeps one stable orchestrator session id across a run's checkpoints."""

    def __init__(self, root, **kwargs):
        super().__init__(root, **kwargs)
        self.orchestrator_sessions: dict[str, list[dict]] = {}

    async def start(self, prompt, repo, **kwargs):
        resumed_session = kwargs.pop("resumed_from_session", None)
        task = await super().start(prompt, repo, **kwargs)
        if resumed_session:
            task.result["session_id"] = resumed_session
        if "Context:\n" in prompt and "Decision" in kwargs.get("title", ""):
            run_name = kwargs["title"].split(" · ")[0]
            context = json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
            if context.get("workflow_scope"):
                run_name = context["workflow_scope"]["workflow_name"]
            self.orchestrator_sessions.setdefault(run_name, []).append({"task_id": task.task_id, "session_id": task.result["session_id"]})
        return task

    async def resume(self, previous, prompt, **kwargs):
        kwargs["resumed_from_session"] = previous.result["session_id"]
        return await super().resume(previous, prompt, **kwargs)


def parent_multi(workflow_ids: list[str], *, mode: str = "child", max_parallel: int = 4) -> dict:
    nodes: list[dict] = [{"id": "start", "type": "start"}, {"id": "split", "type": "parallel_start", "parallel_group_id": "p"}, {"id": "join", "type": "parallel_end", "parallel_group_id": "p"}]
    connections: list[dict] = [{"id": "to-split", "source": "start", "target": "split"}]
    for index, workflow_id in enumerate(workflow_ids):
        nodes.append({"id": f"call{index + 1}", "type": "workflow", "workflow_ref": {"workflow_id": workflow_id}, "orchestrator_mode": mode, "instructions": "Delegate"})
        connections.append({"id": f"c{index}", "source": "split", "target": f"call{index + 1}"})
        connections.append({"id": f"cj{index}", "source": f"call{index + 1}", "target": "join"})
    nodes.append({"id": "end", "type": "end"})
    connections.append({"id": "done", "source": "join", "target": "end"})
    return {"name": "parent", "routing_mode": "explicit", "orchestrator": {"backend": "codex"}, "max_parallel": max_parallel, "nodes": nodes, "connections": connections}


async def run_tree(storage, tmp_path, parent_name: str, registry: TreeRegistry | None = None, *, timeout: float = 20.0):
    definition = storage.get(parent_name)
    tree = refs.resolve_dependencies(storage, definition=definition)
    run = storage.create_run(w.validate_definition(definition), "ORIGINAL PARENT REQUEST", tmp_path, permission_policy="saved_node", dependency_tree=tree)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(runner_policy="guided"), "guided")
    registry = registry or TreeRegistry(storage.root)
    supervisor = w.WorkflowSupervisor(registry, storage)
    await asyncio.wait_for(supervisor.execute(run["workflow_run_id"]), timeout)
    return storage.get_run(run["workflow_run_id"]), registry


def child_runs(storage, run):
    return [r for r in storage.list_runs() if (r.get("parent_link") or {}).get("root_workflow_run_id") == run["workflow_run_id"]]


def invocation_activation(run):
    return next(a for a in run["activations"] if a.get("invocation"))


@pytest.fixture
def two_level(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    child = store.save("child", child_graph("child"))
    store.save("parent", parent_graph(child["workflow_id"]))
    return store


async def test_child_mode_prompt_assignment_and_isolation(two_level, tmp_path):
    run, registry = await run_tree(two_level, tmp_path, "parent")
    assert run["status"] == "completed", run.get("attention_reason")
    children = child_runs(two_level, run)
    assert len(children) == 1
    child = children[0]
    # The child's prompt is exactly the parent orchestrator's assignment.
    assert child["prompt"] == "Focused assignment for call"
    assert child["parent_link"]["workflow_run_id"] == run["workflow_run_id"]
    assert child["parent_link"]["root_workflow_run_id"] == run["workflow_run_id"]
    assert child["orchestrator_mode"] == "child"
    assert "ORIGINAL PARENT REQUEST" not in json.dumps(child)
    # The child has its own orchestrator session, separate from the parent's.
    assert run["sessions"]["orchestrator"]["session_id"] != child["sessions"]["orchestrator"]["session_id"]
    # Workers never see the parent's original request and boot Fresh.
    workers = [call for call in registry.dispatches if call["label"] == "work"]
    assert len(workers) == 1
    assert workers[0]["run"] == "child"
    assert "ORIGINAL PARENT REQUEST" not in workers[0]["prompt"]
    # The parent completes with the child outcome and leaf result refs.
    activation = invocation_activation(run)
    outcome = activation["node_result"]["result"]["child_outcome"]
    assert outcome["kind"] == "completed"
    assert outcome["child_workflow_run_id"] == child["workflow_run_id"]
    assert outcome["final_result_refs"]
    assert all(ref["workflow_run_id"] == child["workflow_run_id"] for ref in outcome["final_result_refs"])


async def test_child_mode_inputs_arrive_complete_and_worker_sees_them(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    child = store.save("child", child_graph("child"))
    store.save("parent", parent_graph(child["workflow_id"], prep=True))
    outputs = {"prep": {"status": "succeeded", "result": {"summary": "Prepared " + "x" * 40000}, "evidence": []}}
    run, registry = await run_tree(store, tmp_path, "parent", TreeRegistry(store.root, outputs=outputs))
    assert run["status"] == "completed", run.get("attention_reason")
    child = child_runs(store, run)[0]
    inputs = child["invocation_inputs"]
    assert inputs[0]["kind"] == "input_results"
    # Complete: the full 40k prep result travels into the child uncapped.
    assert len(json.dumps(inputs[0]["results"][0]["node_result"])) > 40000
    workers = [call for call in registry.dispatches if call["label"] == "work"]
    assert "x" * 1000 in workers[0]["prompt"]


async def test_child_mode_decision_context_shows_pinned_workflow_metadata(two_level, tmp_path):
    class InspectRegistry(TreeRegistry):
        def __init__(self, root):
            super().__init__(root)
            self.parent_contexts = []
        async def start(self, prompt, repo, **kwargs):
            if "Context:\n" in prompt:
                context = json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
                if context["workflow_run_id"] not in {c["workflow_run_id"] for c in self.parent_contexts} or context["current_stage"]["node_id"] == "start":
                    self.parent_contexts.append(context)
            return await super().start(prompt, repo, **kwargs)

    run, registry = await run_tree(two_level, tmp_path, "parent", InspectRegistry(two_level.root))
    assert run["status"] == "completed"
    parent_start = next(c for c in registry.parent_contexts if c["workflow_run_id"] == run["workflow_run_id"] and c["current_stage"]["node_id"] == "start")
    choice = next(c for c in parent_start["valid_continuations"] if c["node_id"] == "call")
    assert choice["workflow"]["name"] == "child"
    assert choice["workflow"]["orchestrator_mode"] == "child"
    assert choice["workflow"]["access"]["max_freedom"] == "publish"
    child_contexts = [c for c in registry.parent_contexts if c["workflow_run_id"] != run["workflow_run_id"]]
    assert child_contexts
    assert all(c.get("workflow_scope") is None for c in child_contexts)
    assert all("ORIGINAL PARENT REQUEST" != c["original_request"] or c["original_request"] == "Focused assignment for call" for c in child_contexts)


async def test_current_mode_checkpoints_resume_owner_session(two_level, tmp_path):
    store = two_level
    child_id = store.get("child")["workflow_id"]
    store.save("parent", parent_graph(child_id, mode="current"), store.get("parent")["revision"])
    registry = SessionRegistry(store.root)
    run, _ = await run_tree(store, tmp_path, "parent", registry)
    assert run["status"] == "completed", run.get("attention_reason")
    child = child_runs(store, run)[0]
    assert child["orchestrator_mode"] == "current"
    assert child["orchestrator_session_owner_run_id"] == run["workflow_run_id"]
    # The child's orchestrator config is the owner's, not the child's own.
    assert child["orchestrator_config"] == run["definition"]["orchestrator"]
    sessions = registry.orchestrator_sessions["child"]
    assert sessions, "the child's orchestrator turns were dispatched"
    assert len({s["session_id"] for s in sessions}) == 1
    owner_session = run["sessions"].get("orchestrator")
    assert owner_session and owner_session["session_id"] == sessions[0]["session_id"]
    all_turns = registry.orchestrator_sessions["parent"] + sessions
    assert len({s["session_id"] for s in all_turns}) == 1
    assert not any(key.startswith("child:") for key in run["sessions"])
    # The child's own decision contexts carry the workflow scope.
    child_turns = [context for context in registry.contexts if (context.get("workflow_scope") or {}).get("child_workflow_run_id") == child["workflow_run_id"]]
    assert child_turns


async def test_current_mode_child_orchestrator_is_never_dispatched(two_level, tmp_path):
    store = two_level
    child_definition = child_graph("child")
    child_definition["orchestrator"] = {"backend": "claude"}
    saved_child = store.save("child", child_definition, store.get("child")["revision"])
    store.save("parent", parent_graph(saved_child["workflow_id"], mode="current"), store.get("parent")["revision"])
    run, registry = await run_tree(store, tmp_path, "parent")
    assert run["status"] == "completed", run.get("attention_reason")
    child = child_runs(store, run)[0]
    # Every orchestrator turn in the tree used the owner's codex candidates.
    decision_backends = {call["kwargs"].get("task_id") and call["label"] for call in registry.dispatches if call["label"] == "Decision"}
    assert registry.dispatches
    # The child run record keeps its own (unused) orchestrator config from the pin.
    assert child["definition"]["orchestrator"]["backend"] == "claude"


async def test_current_mode_workflow_scope_in_child_context(two_level, tmp_path):
    class ScopeRegistry(TreeRegistry):
        def __init__(self, root):
            super().__init__(root)
            self.scopes = []
        async def start(self, prompt, repo, **kwargs):
            if "Context:\n" in prompt:
                context = json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
                if context.get("workflow_scope"):
                    self.scopes.append(context["workflow_scope"])
            return await super().start(prompt, repo, **kwargs)

    store = two_level
    child_id = store.get("child")["workflow_id"]
    store.save("parent", parent_graph(child_id, mode="current"), store.get("parent")["revision"])
    run, registry = await run_tree(store, tmp_path, "parent", ScopeRegistry(store.root))
    assert run["status"] == "completed"
    assert registry.scopes
    scope = registry.scopes[0]
    children = child_runs(store, run)
    assert scope["boundary"] == children[0]["workflow_run_id"]
    assert scope["parent_workflow_run_id"] == run["workflow_run_id"]
    assert scope["parent_node_id"] == "call"
    assert scope["workflow_name"] == "child"
    assert scope["orchestrator_mode"] == "current"
    assert scope["assignment"] == "Focused assignment for call"


async def test_current_of_current_resolves_to_nearest_owner(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    grandchild = store.save("grandchild", child_graph("grandchild"))
    middle = store.save("middle", parent_graph(grandchild["workflow_id"], name="middle", mode="current"))
    store.save("root", parent_graph(middle["workflow_id"], name="root", mode="current"))
    run, registry = await run_tree(store, tmp_path, "root")
    assert run["status"] == "completed", run.get("attention_reason")
    runs = {r["workflow_run_id"]: r for r in store.list_runs()}
    middle_run = next(r for r in runs.values() if r["name"] == "middle")
    grandchild_run = next(r for r in runs.values() if r["name"] == "grandchild")
    # The middle run is a Current child, so it does not run its own orchestrator:
    # the grandchild's session owner is the root.
    assert middle_run["orchestrator_session_owner_run_id"] == run["workflow_run_id"]
    assert grandchild_run["orchestrator_session_owner_run_id"] == run["workflow_run_id"]


async def test_parallel_current_children_never_overlap_turns(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    child = store.save("child", child_graph("child"))
    store.save("parent", parent_graph(child["workflow_id"], mode="current", branches=2))
    registry = SessionRegistry(store.root)
    run, _ = await run_tree(store, tmp_path, "parent", registry)
    assert run["status"] == "completed", run.get("attention_reason")
    assert registry.max_in_flight == 1


async def _start_tree(storage, tmp_path, parent_name: str, registry: TreeRegistry):
    definition = storage.get(parent_name)
    tree = refs.resolve_dependencies(storage, definition=definition)
    run = storage.create_run(w.validate_definition(definition), "ORIGINAL PARENT REQUEST", tmp_path, permission_policy="saved_node", dependency_tree=tree)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(runner_policy="guided"), "guided")
    supervisor = w.WorkflowSupervisor(registry, storage)
    task = asyncio.create_task(supervisor.execute(run["workflow_run_id"]))
    return run, registry, task, supervisor


async def _wait_until(predicate, timeout: float = 10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


async def test_n4_pause_mid_flight_settles_worker_and_ignores_spanned_decision(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    c1 = store.save("c1", child_graph("c1"))
    c2 = store.save("c2", child_graph("c2"))
    store.save("parent", parent_multi([c1["workflow_id"], c2["workflow_id"]], max_parallel=4))
    registry = TreeRegistry(store.root)
    # Hold c1's worker mid-flight, and hold c2's second orchestrator decision.
    registry.hold_when["c1 · work"] = 1
    registry.hold_when["c2 · Decision"] = 2
    run, registry, task, supervisor = await _start_tree(store, tmp_path, "parent", registry)
    assert await _wait_until(lambda: registry.reached.get("c1 · work") and registry.reached.get("c2 · Decision"))
    starts_before_pause = dict(registry.occurrences)
    store.control(run["workflow_run_id"], "pause", instructions="hold")
    # The sibling child's worker settles after the pause.
    registry.gates["c1 · work"].set()
    assert await _wait_until(lambda: any(t.get("status") == "completed" for a in store.get_run(_child_id(store, run, "c1"))["activations"] for t in a.get("tasks", [])))
    # The decision whose turn spanned the pause is ignored, not applied.
    registry.gates["c2 · Decision"].set()
    assert await _wait_until(lambda: any(a.get("decision_ignored") for a in store.get_run(_child_id(store, run, "c2"))["activations"]))
    c2_run = store.get_run(_child_id(store, run, "c2"))
    assert c2_run["status"] == "running"
    assert all(not t.get("selected_connections") for t in c2_run["pending"])
    assert store.get_run(run["workflow_run_id"])["status"] == "paused"
    # No new turns started after the pause.
    assert registry.occurrences == starts_before_pause
    # The paused tree parks every loop; the root supervisor returns cleanly.
    await asyncio.wait_for(task, 10)
    assert store.get_run(run["workflow_run_id"])["status"] == "paused"
    # Resuming restarts scheduling; the ignored decision is re-made and re-entry
    # resumes the same children without consuming attempts.
    store.control(run["workflow_run_id"], "resume")
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(run["workflow_run_id"]), 20)
    final = store.get_run(run["workflow_run_id"])
    assert final["status"] == "completed", final.get("attention_reason")


def _child_id(store, run, name):
    return next(r["workflow_run_id"] for r in store.list_runs() if (r.get("parent_link") or {}).get("root_workflow_run_id") == run["workflow_run_id"] and r["name"] == name)


async def test_n4_uncertain_attempt_holds_the_only_slot_and_the_waiter_returns(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    c1 = store.save("c1", child_graph("c1"))
    c2 = store.save("c2", child_graph("c2"))
    store.save("parent", parent_multi([c1["workflow_id"], c2["workflow_id"]], max_parallel=1))
    registry = TreeRegistry(store.root)
    registry.hold_when["c1 · work"] = 1
    registry.fail_when["c1 · work"] = 1
    run, registry, task, supervisor = await _start_tree(store, tmp_path, "parent", registry)
    try:
        assert await _wait_until(lambda: registry.reached.get("c1 · work")), f"reached={registry.reached} occurrences={registry.occurrences}"
        registry.gates["c1 · work"].set()
        # The uncertain attempt holds the only slot; the tree goes to needs_attention,
        # C2's waiter returns not-started and the supervisor exits cleanly.
        await asyncio.wait_for(task, 15)
        root = store.get_run(run["workflow_run_id"])
        assert root["status"] == "needs_attention"
        c1_run = store.get_run(_child_id(store, run, "c1"))
        uncertain = [t for a in c1_run["activations"] for t in a.get("tasks", []) if t.get("status") == "uncertain"]
        assert uncertain, "the in-flight attempt stayed uncertain"
        assert c1_run["status"] == "needs_attention"
        assert registry.max_in_flight == 1
    finally:
        if not task.done():
            task.cancel()
            try:
                await task
            except BaseException:
                pass


async def test_nested_parallel_inside_child_and_parallel_end_waits_for_child(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    child = store.save("child", child_graph("child", parallel=True, work_count=2))
    store.save("parent", parent_graph(child["workflow_id"]))
    run, registry = await run_tree(store, tmp_path, "parent")
    assert run["status"] == "completed", run.get("attention_reason")
    child_run = child_runs(store, run)[0]
    workers = [call for call in registry.dispatches if call["label"].startswith("work")]
    assert len(workers) == 2
    assert child_run["status"] == "completed"
    # The parent's Parallel end waited for the whole child invocation.
    assert invocation_activation(run)["node_result"]["status"] == "succeeded"


async def test_max_parallel_two_bounds_2x2_workers(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    child = store.save("child", child_graph("child", parallel=True, work_count=2))
    store.save("parent", parent_graph(child["workflow_id"], branches=2, max_parallel=2))
    run, registry = await run_tree(store, tmp_path, "parent")
    assert run["status"] == "completed", run.get("attention_reason")
    workers = [call for call in registry.dispatches if call["label"].startswith("work")]
    assert len(workers) == 4
    assert registry.max_in_flight <= 2


async def test_max_parallel_one_with_two_children_never_deadlocks(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    c1 = store.save("c1", child_graph("c1"))
    c2 = store.save("c2", child_graph("c2"))
    store.save("parent", parent_multi([c1["workflow_id"], c2["workflow_id"]], max_parallel=1))
    run, registry = await run_tree(store, tmp_path, "parent")
    assert run["status"] == "completed", run.get("attention_reason")
    assert registry.max_in_flight == 1
    assert len(child_runs(store, run)) == 2


async def test_no_workflow_nodes_at_max_parallel_one_goes_fresh_and_tries_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    store = w.WorkflowStore(tmp_path)
    graph = {"name": "plain", "routing_mode": "explicit", "max_parallel": 1, "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": "work", "type": "agent", "role": "task", "instructions": "Do the work", "session_mode": "resume", "agent": {"backend": "codex", "max_turns": 5, "fallbacks": [{"backend": "claude"}]}}, {"id": "end", "type": "end"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]}
    saved = store.save("plain", graph)
    run_record, registry = await run_tree(store, tmp_path, "plain")
    assert run_record["status"] == "completed", run_record.get("attention_reason")
    workers = [call for call in registry.dispatches if call["label"] == "work"]
    # The turn-capped codex candidate was refused and the claude fallback ran,
    # with no retained session to resume: a Fresh bootstrap.
    assert workers and workers[0]["kwargs"]["task_id"]
    assert saved["nodes"][1]["session_mode"] == "resume"


async def test_read_only_child_uses_writer_roots_pooled_checkout_strength(tmp_path, monkeypatch):
    # TreeRegistry simulates dispatch; this lease regression requires no installed CLI.
    monkeypatch.setattr(w.backends, 'is_installed', lambda backend: True)
    monkeypatch.setattr(w, '_launch', lambda *args: None)
    storage = w.WorkflowStore(tmp_path)
    child = child_graph()
    next(n for n in child['nodes'] if n['id'] == 'work')['freedom'] = 'read_only'
    saved = storage.save('child', child)
    storage.save('parent', parent_graph(saved['workflow_id'], prep=True))
    run, registry = await run_tree(storage, tmp_path, 'parent')
    assert run['status'] == 'completed', run.get('attention_reason')
    assert any(call['run'] == 'child' and call['label'] == 'work' and call['kwargs']['freedom'] == 'read_only' for call in registry.dispatches)


async def test_pooled_checkout_lease_is_shared_and_another_tree_waits(tmp_path):
    from polybridge.workflow_invocation import WorkflowTree

    store = w.WorkflowStore(tmp_path)
    repo = tmp_path / "repo"
    repo.mkdir()
    tree_one = WorkflowTree(store, permits=1)
    tree_two = WorkflowTree(store, permits=1)
    supervisor_one = w.WorkflowSupervisor(None, store, tree=tree_one)
    supervisor_two = w.WorkflowSupervisor(None, store, tree=tree_two)
    assert supervisor_one.checkout_leases is tree_one.leases
    assert supervisor_two.checkout_leases is tree_two.leases
    waits: list[dict] = []
    async with w.CheckoutLease(store, repo, True, pool=tree_one.leases):
        key = next(iter(tree_one.leases))
        assert tree_one.leases[key]["users"] == 1
        # A second supervisor in the same tree joins the pooled descriptor.
        async with w.CheckoutLease(store, repo, True, pool=tree_one.leases):
            assert tree_one.leases[key]["users"] == 2
        # A different tree waits for the OS lease.
        other = asyncio.create_task(_acquire_lease(store, repo, tree_two, waits))
        await asyncio.sleep(0.3)
        assert waits, "the other tree is waiting for the checkout lease"
        other.cancel()
        try:
            await other
        except (asyncio.CancelledError, Exception):
            pass


async def _acquire_lease(store, repo, tree, waits):
    def on_wait(detail):
        waits.append(detail)
    async with w.CheckoutLease(store, repo, True, on_wait=on_wait, wait_seconds=30.0, pool=tree.leases):
        pass
