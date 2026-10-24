"""Deterministic traversal guardrails and transition accounting."""

import pytest

from harflow_mcp import workflow_delegation as d
from harflow_mcp import workflows as w
from harflow_mcp.workflow_traversal import automatic_edges, child_start_assignment

from test_workflow_traversal import definition, policy
from test_workflow_explicit_parallel import explicit
from test_workflow_run_node_execution import assignment_for
from test_workflow_delegation import Registry, default_decision, graph as serial_graph, run_flow, storage  # noqa: F401


def planning_parallel_definition():
    return {
        "name": "planned-parallel",
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "plan", "type": "agent", "role": "planning", "instructions": "Produce two implementation tasks", "agent": {"backend": "codex"}},
            {"id": "split", "type": "parallel_start", "parallel_group_id": "implementation"},
            {"id": "impl-a", "type": "agent", "role": "implementation", "instructions": "Implement task one", "agent": {"backend": "codex"}},
            {"id": "impl-b", "type": "agent", "role": "implementation", "instructions": "Implement task two", "agent": {"backend": "codex"}},
            {"id": "join", "type": "parallel_end", "parallel_group_id": "implementation"},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "to-plan", "source": "start", "target": "plan"},
            {"id": "to-split", "source": "plan", "target": "split"},
            {"id": "impl-a", "source": "split", "target": "impl-a"},
            {"id": "impl-b", "source": "split", "target": "impl-b"},
            {"id": "a-join", "source": "impl-a", "target": "join"},
            {"id": "b-join", "source": "impl-b", "target": "join"},
            {"id": "finish", "source": "join", "target": "end"},
        ],
    }


def _worker_run(node_id="left", **updates):
    value = {
        "runner_policy": "guided",
        "definition": w.validate_definition(definition()),
        "activations": [],
        "joins": {},
        "transitions": 0,
    }
    value.update(updates)
    value["joins"] = {"pair": {"join_id": "join", "split_id": "split", "expected": 2, "arrived": [], "selected_connection_ids": ["left", "right"]}}
    node = next(item for item in value["definition"]["nodes"] if item["id"] == node_id)
    activation_id = "worker"
    value["activations"] = [{
        "id": activation_id,
        "node_id": node_id,
        "role": "node",
        "status": "completed",
        "tasks": [],
        "node_result": {"status": "succeeded", "result": {}},
    }]
    token = {"id": "token", "node_id": node_id, "execution_activation_id": activation_id, "execution_complete": True, "stack": ["pair"], "branch_ids": {"pair": "left"}, "result": {"status": "succeeded", "result": {}}}
    return value, node, token


def _linked_child_start(mode="child"):
    prompt = "  Résumé child assignment\nkeep this spacing  "
    definition = w.validate_definition({
        "name": "linked-child",
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "task", "type": "agent", "role": "task", "purpose": "Perform the task", "instructions": "Task instructions", "agent": {"backend": "codex"}},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "to-task", "source": "start", "target": "task"},
            {"id": "to-end", "source": "task", "target": "end"},
        ],
    })
    run = {
        "definition": definition,
        "orchestrator_mode": mode,
        "parent_link": {"workflow_run_id": "parent"},
        "prompt": prompt,
        "tasks": [],
        "activations": [],
        "decisions": [],
        "pending": [{"id": "token", "node_id": "start"}],
        "joins": {},
        "transitions": 0,
        "runner_policy": "guided",
    }
    node = next(item for item in definition["nodes"] if item["id"] == "start")
    return run, node, run["pending"][0], prompt


@pytest.mark.parametrize("mode", ["current", "child"])
def test_child_start_assignment_passes_exact_prompt_and_selected_connection(mode):
    run, node, token, prompt = _linked_child_start(mode)
    transition = child_start_assignment(run, node, token)
    assert transition and transition["connections"][0]["id"] == "to-task"
    assert transition["assignments"]["to-task"]["assignment_prompt"] == prompt
    assert transition["reason"].startswith("Harflow passed the invocation assignment")


@pytest.mark.parametrize("mutation", [
    "no_parent", "start_always_route", "target_always_route", "multiple_edges",
    "parallel_target", "planning_target", "inherited_binding", "resume_selection",
    "parent_goal", "parent_tasks", "artifact_refs",
])
def test_child_start_assignment_rejects_ambiguous_or_inherited_scope(mutation):
    run, node, token, _ = _linked_child_start()
    if mutation == "no_parent":
        run.pop("parent_link")
    elif mutation == "start_always_route":
        node["always_route"] = True
    elif mutation == "target_always_route":
        run["definition"]["nodes"][1]["always_route"] = True
    elif mutation == "multiple_edges":
        run["definition"]["connections"].append({"id": "other", "source": "start", "target": "end"})
    elif mutation == "parallel_target":
        run["definition"]["nodes"][1]["type"] = "parallel_start"
    elif mutation == "planning_target":
        run["definition"]["nodes"][1]["role"] = "planning"
    elif mutation == "inherited_binding":
        run["inherited_orchestrator_binding"] = {"source_task_id": "owner"}
    elif mutation == "resume_selection":
        run["child_session_selection"] = {"selected_mode": "resume"}
    elif mutation == "parent_goal":
        run["parent_goal"] = {"objective": "goal"}
    elif mutation == "parent_tasks":
        run["tasks"] = [{"id": "task", "status": "pending"}]
    elif mutation == "artifact_refs":
        run["invocation_artifact_refs"] = ["artifact:one"]
    assert child_start_assignment(run, node, token) is None


def test_always_route_keeps_clean_worker_at_routing_checkpoint():
    run, node, token = _worker_run()
    assert automatic_edges(run, node, token)
    node["always_route"] = True
    assert automatic_edges(run, node, token) is None


def test_clean_worker_with_conditional_or_backward_edge_keeps_judgment():
    run, node, token = _worker_run()
    assert automatic_edges(run, node, token)
    edge = next(edge for edge in run["definition"]["connections"] if edge["source"] == "left")
    edge["condition"] = "Only if evidence is complete"
    assert automatic_edges(run, node, token) is None
    edge.pop("condition")
    edge["backward"] = True
    assert automatic_edges(run, node, token) is None


def test_failed_or_uncertain_worker_never_traverses_structurally():
    run, node, token = _worker_run()
    assert automatic_edges(run, node, token)
    token["result"]["status"] = "failed"
    token["failed_execution_refs"] = ["failed"]
    run["activations"] = [{"id": "failed", "status": "failed", "tasks": [], "optional_failure": False}]
    assert automatic_edges(run, node, token) is None
    token["result"]["status"] = "succeeded"
    token["result"]["outcome_unknown"] = True
    assert automatic_edges(run, node, token) is None


def test_transition_budget_is_checked_by_runtime_advance_not_selection_helper():
    run, node, token = _worker_run(node_id="join")
    run["definition"]["max_transitions"] = 0
    run["transitions"] = 0
    # The helper only identifies a structurally safe edge; the supervisor's
    # atomic advance enforces the transition budget before persistence.
    assert automatic_edges(run, node, token)[0]["target"] == "end"


async def test_parallel_join_to_executable_has_one_join_checkpoint(storage, tmp_path):
    graph = definition()
    graph["nodes"].insert(-1, {"id": "merge", "type": "agent", "instructions": "Merge both branches", "agent": {"backend": "codex"}})
    finish = next(edge for edge in graph["connections"] if edge["id"] == "finish")
    finish["target"] = "merge"
    graph["connections"].append({"id": "merge-finish", "source": "merge", "target": "end"})
    run, registry = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy=policy), guided=True)
    assert run["status"] == "completed"
    stages = [context["current_stage"]["node_id"] for context in registry.contexts]
    assert stages.count("join") == 1
    assert stages[0] == "start"
    assert stages[-1] == "end"
    workers = [activation["node_id"] for activation in run["activations"] if activation["role"] == "node"]
    assert workers.count("left") == workers.count("right") == workers.count("merge") == 1


async def test_planned_parallel_implementations_carry_task_evidence_through_join(storage, tmp_path):
    def policy(context, registry):
        decision = default_decision(context, registry)
        if context["current_stage"]["node_id"] == "plan":
            entry = decision["next"][0]
            entry["branch_assignments"] = [
                {"continuation_id": "impl-a", "prompt": "Implement task one", "session_mode": "fresh", "assigned_task_ids": ["one"]},
                {"continuation_id": "impl-b", "prompt": "Implement task two", "session_mode": "fresh", "assigned_task_ids": ["two"]},
            ]
        if context["current_stage"]["node_id"] == "end":
            decision["task_updates"] = [
                {"task_id": "one", "status": "completed", "reason": "Implementation evidence one"},
                {"task_id": "two", "status": "completed", "reason": "Implementation evidence two"},
            ]
        return decision

    outputs = {
        "plan": {"status": "succeeded", "result": {"tasks": [{"id": "one", "title": "One"}, {"id": "two", "title": "Two"}], "technical_plan": "Implement both"}, "evidence": []},
        "impl-a": {"status": "succeeded", "result": {"completed_task_ids": ["one"]}, "evidence": []},
        "impl-b": {"status": "succeeded", "result": {"completed_task_ids": ["two"]}, "evidence": []},
    }
    run, registry = await run_flow(storage, tmp_path, planning_parallel_definition(), Registry(storage.root, policy=policy, outputs=outputs), guided=True)
    assert run["status"] == "completed", run.get("attention_reason")
    assert [context["current_stage"]["node_id"] for context in registry.contexts] == ["start", "plan", "end"]
    assert not any(context["current_stage"]["node_id"] in {"impl-a", "impl-b"} for context in registry.contexts)
    workers = [activation for activation in run["activations"] if activation["role"] == "node"]
    assert {activation["node_id"] for activation in workers} == {"plan", "impl-a", "impl-b"}
    assert {task_id for activation in workers for task_id in activation.get("assigned_task_ids", [])} == {"one", "two"}
    assert {task["id"] for task in run["tasks"] if task["status"] == "completed"} >= {"one", "two"}
    tasks = {task["id"]: task for task in run["tasks"]}
    implementations = {activation["node_id"]: activation["id"] for activation in workers if activation["node_id"].startswith("impl-")}
    assert tasks["one"]["completed_by_activation_id"] == implementations["impl-a"]
    assert tasks["two"]["completed_by_activation_id"] == implementations["impl-b"]
    assert any(record.get("actor") == "harflow" and record.get("node_id") == "join" for record in run["decisions"])


@pytest.mark.parametrize("invalid", ["unassigned", "unreferenced", "failed", "optional", "wrong_role"])
def test_task_update_rejects_historical_or_unqualified_evidence(monkeypatch, invalid):
    activation = {
        "id": "impl",
        "role": "node",
        "node_id": "impl-a",
        "status": "completed",
        "tasks": [],
        "assigned_task_ids": ["one"],
        "node_result": {"status": "succeeded", "result": {"completed_task_ids": ["one"]}},
    }
    token = {"id": "end-token", "execution_complete": True, "execution_activation_id": "impl", "input_result_refs": ["impl"], "decision_id": "decision"}
    if invalid == "unassigned":
        activation["assigned_task_ids"] = []
    elif invalid == "unreferenced":
        token["input_result_refs"] = []
    elif invalid == "failed":
        activation["status"] = "failed"
    elif invalid == "optional":
        activation["optional_failure"] = True
    elif invalid == "wrong_role":
        activation["role"] = "orchestrator"
    run = {"runner_policy": "guided", "definition": {"nodes": [{"id": "end", "type": "end"}], "connections": []}, "activations": [activation], "tasks": [{"id": "one", "status": "pending"}], "pending": [{"id": "end-token"}], "joins": {}}
    monkeypatch.setattr(d, "continuations", lambda *args, **kwargs: [])
    decision = {"decision_id": "decision", "action": "complete", "reason": "Claimed evidence", "task_updates": [{"task_id": "one", "status": "completed", "reason": "Claimed"}]}
    with pytest.raises(w.WorkflowError, match="Completion requires"):
        d.validate_decision(run, run["definition"]["nodes"][0], token, decision, False)


async def test_failed_branch_retries_while_sibling_is_running_then_arrives_without_branch_decision(storage, tmp_path):
    class DelayedSiblingRegistry(Registry):
        def __init__(self, root, policy, outputs):
            super().__init__(root, policy, outputs)
            import asyncio
            self.right_started = asyncio.Event()
            self.release_right = asyncio.Event()
            self.right_done = False
            self.retry_seen_while_right_running = False

        async def start(self, prompt, repo, **kwargs):
            if kwargs.get("title", "").endswith(" · right"):
                self.right_started.set()
                await self.release_right.wait()
                self.right_done = True
            return await super().start(prompt, repo, **kwargs)

    attempts = 0

    def left2_output(prompt, kwargs):
        nonlocal attempts
        attempts += 1
        return {"status": "blocked" if attempts == 1 else "succeeded", "result": {"summary": "left2"}, "evidence": []}

    graph = explicit()
    graph["max_parallel"] = 2
    for node in graph["nodes"]:
        node["title"] = node["id"]

    def retry_policy(context, registry):
        retry = next((choice for choice in context["valid_continuations"] if choice["kind"] == "retry_execution"), None)
        if retry:
            registry.retry_seen_while_right_running = not registry.right_done
            registry.release_right.set()
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Retry the failed branch", "next": [{"continuation_id": retry["continuation_id"], "prompt": "Corrected assignment", "session_mode": "fresh"}]}
        decision = default_decision(context, registry)
        for entry, choice in zip(decision.get("next", []), context["valid_continuations"]):
            if "branch_continuations" in choice:
                entry["branch_assignments"] = [assignment_for(branch) for branch in choice["branch_continuations"]]
        return decision

    registry = DelayedSiblingRegistry(storage.root, retry_policy, {"left2": left2_output})
    run, _ = await run_flow(storage, tmp_path, graph, registry, guided=True, timeout_seconds=10)
    assert run["status"] == "completed", run.get("attention_reason")
    assert registry.right_started.is_set() and registry.retry_seen_while_right_running
    assert not run["joins"]
    assert attempts == 2
    assert sum(activation["node_id"] == "right" for activation in run["activations"] if activation["role"] == "node") == 1


async def test_transition_cap_blocks_runtime_automatic_advance_without_runner_record(storage, tmp_path):
    graph = definition()
    graph["max_transitions"] = 1
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy=policy), guided=True)
    assert run["status"] == "needs_attention"
    assert run["transitions"] == 0
    assert not [record for record in run["decisions"] if record.get("actor") == "harflow"]


async def test_transition_cap_blocks_serial_automatic_exit_after_source_worker(storage, tmp_path):
    graph = serial_graph()
    graph["max_transitions"] = 1
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy=default_decision), guided=True)
    assert run["status"] == "needs_attention"
    assert run["transitions"] == 1
    assert [activation["node_id"] for activation in run["activations"] if activation["role"] == "node" and activation["status"] == "completed"] == ["work"]
    assert not [record for record in run["decisions"] if record.get("actor") == "harflow"]


async def test_reexecuting_committed_structural_run_is_idempotent(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, definition(), Registry(storage.root, policy=policy), guided=True)
    runner_before = [record for record in run["decisions"] if record.get("actor") == "harflow"]
    workers_before = [activation for activation in run["activations"] if activation["role"] == "node"]
    supervisor = w.WorkflowSupervisor(registry, storage)
    await supervisor.execute(run["workflow_run_id"])
    replayed = storage.get_run(run["workflow_run_id"])
    runner_after = [record for record in replayed["decisions"] if record.get("actor") == "harflow"]
    workers_after = [activation for activation in replayed["activations"] if activation["role"] == "node"]
    assert replayed["status"] == "completed"
    assert len(runner_after) == len(runner_before)
    assert len(workers_after) == len(workers_before)
    assert replayed["transitions"] == run["transitions"]


async def test_recovery_after_committed_automatic_transition_does_not_repeat_dispatch(storage, tmp_path):
    run = storage.create_run(w.validate_definition(definition()), "Crash after structural commit", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda current: current.update(runner_policy="guided"), "guided")
    registry = Registry(storage.root, policy=policy)
    supervisor = w.WorkflowSupervisor(registry, storage)
    original_update = supervisor.update
    crashed = False

    def interrupt_after_runner_commit(mutator, reason, details=None):
        nonlocal crashed
        result = original_update(mutator, reason, details)
        if not crashed and any(record.get("actor") == "harflow" for record in supervisor.run()["decisions"]):
            crashed = True
            raise RuntimeError("synthetic interruption after committed automatic transition")
        return result

    supervisor.update = interrupt_after_runner_commit
    await supervisor.execute(run["workflow_run_id"])
    assert crashed, supervisor.run()
    interrupted = storage.get_run(run["workflow_run_id"])
    runner_before = [record for record in interrupted["decisions"] if record.get("actor") == "harflow"]
    workers_before = [activation for activation in interrupted["activations"] if activation["role"] == "node"]
    def resume_interrupted(current):
        current["status"] = "running"
        current.pop("attention_reason", None)
    storage.update_run(run["workflow_run_id"], resume_interrupted, "recover_after_interrupt")
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    recovered = storage.get_run(run["workflow_run_id"])
    runner_after = [record for record in recovered["decisions"] if record.get("actor") == "harflow"]
    workers_after = [activation for activation in recovered["activations"] if activation["role"] == "node"]
    assert recovered["status"] == "completed", recovered.get("attention_reason")
    assert {record["decision_id"] for record in runner_before} <= {record["decision_id"] for record in runner_after}
    assert len({record["decision_id"] for record in runner_after}) == len(runner_after)
    assert {activation["id"] for activation in workers_before} <= {activation["id"] for activation in workers_after}
    assert [activation["node_id"] for activation in workers_after] == ["left", "right"]
    assert recovered["transitions"] == 6
    assert sum(record["decision_id"] == runner_before[0]["decision_id"] for record in runner_after) == 1
