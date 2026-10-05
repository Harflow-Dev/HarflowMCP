"""Run workflow node outcomes, control, recovery and question forwarding."""
import asyncio
import copy
import uuid

import pytest

from polybridge import workflow_delegation as d
from polybridge import workflow_invocation as inv
from polybridge import workflow_references as refs
from polybridge import workflows as w

from test_workflow_run_node_execution import (TreeRegistry, _child_id, _start_tree, _wait_until, assignment_for, child_graph, child_runs, invocation_activation, parent_graph, run_tree, workflow_policy)


def write_run(store, run):
    w._write(store.runs / f"{run['workflow_run_id']}.json", run)


def seed_parent(store, tmp_path, parent_name, *, stage, child_id=None, child_run=None):
    """Craft a parent run interrupted at one workflow-node invocation."""
    definition = store.get(parent_name)
    tree = refs.resolve_dependencies(store, definition=definition)
    run = store.create_run(w.validate_definition(definition), "ORIGINAL PARENT REQUEST", tmp_path, permission_policy="saved_node", dependency_tree=tree)
    token = {"id": uuid.uuid4().hex, "node_id": "call", "stack": [], "context": {}, "assignment_prompt": "Focused assignment for call", "input_result_refs": []}
    activation = {"id": uuid.uuid4().hex, "node_id": "call", "role": "node", "status": "running", "tasks": [], "created_at": 0.0, "token": copy.deepcopy(token), "invocation": {"child_workflow_run_id": child_id or uuid.uuid4().hex, "stage": stage, "workflow_id": definition["nodes"][1]["workflow_ref"]["workflow_id"], "workflow_name": "child", "orchestrator_mode": "child", "timeout_seconds": None, "timeout_elapsed_seconds": 0}}
    token["execution_activation_id"] = activation["id"]
    def seed(r):
        r.update(pending=[token], activations=[activation], execution_initialized=True, status="running", runner_policy="guided")
    store.update_run(run["workflow_run_id"], seed, "seeded")
    if child_run is not None:
        write_run(store, child_run)
    return store.get_run(run["workflow_run_id"])


def finished_child(store, parent_run, activation, *, status="completed", summary="Done", failed_decision=None, executions=(), permission_denials=None):
    """A synthetic settled child run linked to one parent invocation."""
    child_id = activation["invocation"]["child_workflow_run_id"]
    child_definition = parent_run["dependency_tree"]["workflows"][activation["invocation"]["workflow_id"]]["definition"]
    child = {"workflow_run_id": child_id, "kind": "workflow", "name": "child", "definition": child_definition, "workflow_id": activation["invocation"]["workflow_id"], "revision": 0, "prompt": "Focused assignment for call", "repo_path": parent_run["repo_path"], "freedom": "unrestricted", "network": None, "status": status, "created_at": 0.0, "updated_at": 0.0, "sequence": 0, "transitions": 0, "activations": [], "decisions": [], "sessions": {}, "suppressed_candidates": [], "pending": [], "joins": {}, "instructions": "", "attempt_grants": {}, "supervisor_pid": None, "permission_policy": "saved_node", "execution_contract": "delegation", "runner_policy": "guided", "execution_policy": "visit", "retry_counts": {}, "retry_grants": {}, "tasks": [], "parent_link": {"workflow_run_id": parent_run["workflow_run_id"], "execution_id": activation["id"], "node_id": "call", "root_workflow_run_id": parent_run["workflow_run_id"], "depth": 2}, "interaction_owner": "caller", "invocation_inputs": []}
    if failed_decision:
        child["failed_decision_id"] = failed_decision
        child["failure_reason"] = "Child orchestrator failed the objective"
    if status == "completed":
        child["summary"] = summary
    for index, (node_id, node_status, result) in enumerate(executions):
        activation_entry = {"id": f"exec-{index}", "node_id": node_id, "role": "node", "status": "completed" if node_status == "succeeded" else "failed", "tasks": [{"task_id": f"task-{index}", "status": "completed" if node_status == "succeeded" else "failed", "result": result}], "created_at": 0.0, "token": {"id": f"token-{index}"}}
        if node_status == "blocked":
            activation_entry["status"] = "failed"
            activation_entry["node_result"] = {"status": "blocked", "result": {"blocker_category": "permission", "failure_kind": result.get("failure_kind", "permission")}, "evidence": []}
        elif node_status == "succeeded":
            activation_entry["node_result"] = {"status": "succeeded", "result": result, "evidence": []}
        child["activations"].append(activation_entry)
    if permission_denials:
        child["activations"].append({"id": "exec-denied", "node_id": "work", "role": "node", "status": "failed", "tasks": [{"task_id": "task-denied", "status": "failed", "result": {"status": "failed", "permission_denials": permission_denials}}], "created_at": 0.0, "node_result": {"status": "blocked", "result": {"blocker_category": "permission", "failure_kind": "permission"}, "evidence": []}, "token": {"id": "token-denied"}})
    return child


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    s = w.WorkflowStore(tmp_path)
    child = s.save("child", child_graph("child"))
    s.save("parent", parent_graph(child["workflow_id"]))
    return s


def outcome_for(store, parent_run, activation, *, child=None, invocation_updates=None):
    if child is not None:
        write_run(store, child)
    if invocation_updates:
        activation = copy.deepcopy(activation)
        activation["invocation"].update(invocation_updates)
    node = next(n for n in parent_run["definition"]["nodes"] if n["id"] == "call")
    return inv.outcome_for_child(store, parent_run, activation, node)


async def test_outcome_precedence_first_match_wins(store, tmp_path):
    parent_run = store.create_run(w.validate_definition(store.get("parent")), "go", tmp_path, permission_policy="saved_node", dependency_tree=refs.resolve_dependencies(store, definition=store.get("parent")))
    activation = {"id": "a1", "node_id": "call", "invocation": {"child_workflow_run_id": "c1", "workflow_id": parent_run["definition"]["nodes"][1]["workflow_ref"]["workflow_id"], "stage": "settled"}}

    completed = finished_child(store, parent_run, activation, status="completed")
    assert outcome_for(store, parent_run, activation, child=completed)["result"]["child_outcome"]["kind"] == "completed"

    cancelled = finished_child(store, parent_run, activation, status="cancelled")
    assert outcome_for(store, parent_run, activation, child=cancelled)["result"]["child_outcome"]["kind"] == "cancelled"

    failed_decision = finished_child(store, parent_run, activation, status="failed", failed_decision="d1")
    assert outcome_for(store, parent_run, activation, child=failed_decision)["result"]["child_outcome"]["kind"] == "child_failed"

    runtime = finished_child(store, parent_run, activation, status="failed")
    assert outcome_for(store, parent_run, activation, child=runtime)["result"]["child_outcome"]["kind"] == "runtime"

    permission = finished_child(store, parent_run, activation, status="failed", permission_denials=[{"tool": "Bash", "reason": "denied"}])
    assert outcome_for(store, parent_run, activation, child=permission)["result"]["child_outcome"]["kind"] == "permission"

    timeout = finished_child(store, parent_run, activation, status="cancelled")
    result = outcome_for(store, parent_run, activation, child=timeout, invocation_updates={"timeout_expired": True, "timeout_confirmed": True, "timeout_reason": "too slow", "stage": "settled"})
    assert result["result"]["child_outcome"]["kind"] == "timeout"
    assert result["result"]["failure_kind"] == "timeout"

    missing_activation = {"id": "a2", "node_id": "call", "invocation": {"child_workflow_run_id": "never-created", "workflow_id": parent_run["definition"]["nodes"][1]["workflow_ref"]["workflow_id"], "stage": "settled"}}
    missing = outcome_for(store, parent_run, missing_activation, child=None)
    assert missing["result"]["child_outcome"]["kind"] == "uncertain"


async def test_completed_child_with_incidental_denials_still_succeeds(store, tmp_path):
    parent_run = store.create_run(w.validate_definition(store.get("parent")), "go", tmp_path, permission_policy="saved_node", dependency_tree=refs.resolve_dependencies(store, definition=store.get("parent")))
    activation = {"id": "a1", "node_id": "call", "invocation": {"child_workflow_run_id": "c1", "workflow_id": parent_run["definition"]["nodes"][1]["workflow_ref"]["workflow_id"], "stage": "settled"}}
    child = finished_child(store, parent_run, activation, status="completed", executions=[("work", "succeeded", {"summary": "Done", "incidental": True})])
    child["activations"][0]["tasks"][0]["result"]["permission_denials"] = [{"tool": "Bash", "reason": "read denied"}]
    result = outcome_for(store, parent_run, activation, child=child)
    assert result["result"]["child_outcome"]["kind"] == "completed"


async def test_unresolved_permission_block_then_failed_is_permission(store, tmp_path):
    parent_run = store.create_run(w.validate_definition(store.get("parent")), "go", tmp_path, permission_policy="saved_node", dependency_tree=refs.resolve_dependencies(store, definition=store.get("parent")))
    activation = {"id": "a1", "node_id": "call", "invocation": {"child_workflow_run_id": "c1", "workflow_id": parent_run["definition"]["nodes"][1]["workflow_ref"]["workflow_id"], "stage": "settled"}}
    child = finished_child(store, parent_run, activation, status="failed", executions=[("work", "blocked", {"failure_kind": "permission", "permission_denials": [{"tool": "Bash", "reason": "write denied"}]})])
    result = outcome_for(store, parent_run, activation, child=child)
    assert result["result"]["child_outcome"]["kind"] == "permission"
    assert result["result"]["child_outcome"]["permission_evidence"]


async def test_resolved_block_then_completion_is_not_permission(store, tmp_path):
    parent_run = store.create_run(w.validate_definition(store.get("parent")), "go", tmp_path, permission_policy="saved_node", dependency_tree=refs.resolve_dependencies(store, definition=store.get("parent")))
    activation = {"id": "a1", "node_id": "call", "invocation": {"child_workflow_run_id": "c1", "workflow_id": parent_run["definition"]["nodes"][1]["workflow_ref"]["workflow_id"], "stage": "settled"}}
    child = finished_child(store, parent_run, activation, status="failed", executions=[("work", "blocked", {"failure_kind": "permission"})])
    # A later execution resolved the blocked one.
    child["activations"][0]["resolved_by_execution_id"] = "exec-ok"
    child["activations"].append({"id": "exec-ok", "node_id": "work", "role": "node", "status": "completed", "tasks": [{"task_id": "t2", "status": "completed", "result": {"status": "completed"}}], "created_at": 0.0, "node_result": {"status": "succeeded", "result": {"summary": "resolved"}, "evidence": []}, "token": {"id": "tk2"}, "retry_of_execution_id": "exec-0"})
    child["status"] = "completed"
    child["summary"] = "resolved anyway"
    result = outcome_for(store, parent_run, activation, child=child)
    assert result["result"]["child_outcome"]["kind"] == "completed"


async def test_recursive_grandchild_permission_surfaces(store, tmp_path):
    parent_run = store.create_run(w.validate_definition(store.get("parent")), "go", tmp_path, permission_policy="saved_node", dependency_tree=refs.resolve_dependencies(store, definition=store.get("parent")))
    activation = {"id": "a1", "node_id": "call", "invocation": {"child_workflow_run_id": "c1", "workflow_id": parent_run["definition"]["nodes"][1]["workflow_ref"]["workflow_id"], "stage": "settled"}}
    child = finished_child(store, parent_run, activation, status="failed")
    # The child failed through its own Run workflow node whose grandchild hit permission.
    child["activations"].append({"id": "nested", "node_id": "inner-call", "role": "node", "status": "failed", "tasks": [], "created_at": 0.0, "invocation": {"child_workflow_run_id": "g1", "stage": "settled"}, "node_result": {"status": "blocked", "result": {"child_outcome": {"kind": "permission"}, "failure_kind": "permission"}, "evidence": []}, "token": {"id": "tkn"}})
    grandchild = copy.deepcopy(child)
    grandchild.update(workflow_run_id="g1", activations=[])
    grandchild["parent_link"].update(workflow_run_id=child["workflow_run_id"], execution_id="nested")
    write_run(store, grandchild)
    result = outcome_for(store, parent_run, activation, child=child)
    assert result["result"]["child_outcome"]["kind"] == "permission"


async def test_invocation_helpers_refuse_repair_and_caller_blocks(store):
    invocation_activation = {"id": "a", "status": "failed", "tasks": [], "invocation": {"stage": "settled", "timeout_expired": True, "timeout_confirmed": True}, "node_result": {"status": "failed", "result": {"failure_kind": "timeout"}}}
    assert inv.invocation_settled_timeout(invocation_activation) is True
    assert d.caller_decision_block(invocation_activation) is False
    assert d.protocol_repair_eligible({"runner_policy": "guided"}, {"id": "n"}, invocation_activation) is False
    for kind in ("cancelled", "uncertain", "permission"):
        assert inv.invocation_retry_eligible({"node_result": {"result": {"child_outcome": {"kind": kind}}}}) is False
    for kind in ("completed", "timeout", "runtime", "child_failed"):
        assert inv.invocation_retry_eligible({"node_result": {"result": {"child_outcome": {"kind": kind}}}}) is True


async def test_public_controls_on_a_child_id_are_refused_with_the_root_id(store, tmp_path):
    run, registry = await run_tree(store, tmp_path, "parent")
    child = child_runs(store, run)[0]
    for action in ("pause", "resume", "cancel", "recover"):
        with pytest.raises(w.WorkflowError, match=run["workflow_run_id"]):
            store.control(child["workflow_run_id"], action, instructions="reason" if action == "recover" else None)


async def test_cancel_propagates_to_descendants_and_the_root_settles(store, tmp_path, identities):
    import os
    identities.alive(os.getpid())
    registry = TreeRegistry(store.root)
    registry.hold_when["child · work"] = 1
    run, registry, task, supervisor = await _start_tree(store, tmp_path, "parent", registry)
    try:
        assert await _wait_until(lambda: registry.reached.get("child · work"))
        store.control(run["workflow_run_id"], "cancel")
        registry.gates["child · work"].set()
        await asyncio.wait_for(task, 15)
        final = store.get_run(run["workflow_run_id"])
        assert final["status"] == "cancelled"
        child = child_runs(store, final)[0]
        assert child["status"] == "cancelled"
        outcome = invocation_activation(final)["node_result"]["result"]["child_outcome"]
        assert outcome["kind"] == "cancelled"
    finally:
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), 5)
            except BaseException:
                pass


async def test_timeout_accrues_only_while_the_child_executes(store, tmp_path):
    registry = TreeRegistry(store.root)
    definition = store.get("parent")
    definition["nodes"][1]["timeout_seconds"] = 30
    store.save("parent", definition, definition["revision"])
    registry.hold_when["child · work"] = 1
    run, registry, task, supervisor = await _start_tree(store, tmp_path, "parent", registry)
    try:
        assert await _wait_until(lambda: registry.reached.get("child · work"))
        await asyncio.sleep(0.4)
        store.control(run["workflow_run_id"], "pause", instructions="hold")
        invocation = invocation_activation(store.get_run(run["workflow_run_id"]))["invocation"]
        elapsed_while_running = invocation.get("timeout_elapsed_seconds", 0)
        assert elapsed_while_running > 0
        registry.gates["child · work"].set()
        assert await _wait_until(lambda: store.get_run(run["workflow_run_id"])["status"] == "paused")
        invocation = invocation_activation(store.get_run(run["workflow_run_id"]))["invocation"]
        await asyncio.sleep(0.4)
        parked = invocation_activation(store.get_run(run["workflow_run_id"]))["invocation"]
        assert parked.get("timeout_elapsed_seconds", 0) >= elapsed_while_running
        assert parked.get("timeout_elapsed_seconds", 0) < 25
    finally:
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), 5)
            except BaseException:
                pass


async def test_recovery_crash_before_child_file_creates_exactly_one_child(store, tmp_path):
    parent_run = seed_parent(store, tmp_path, "parent", stage="preparing")
    registry = TreeRegistry(store.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(parent_run["workflow_run_id"]), 15)
    final = store.get_run(parent_run["workflow_run_id"])
    assert final["status"] == "completed", final.get("attention_reason")
    children = child_runs(store, final)
    assert len(children) == 1
    assert [call for call in registry.dispatches if call["label"] == "work"], "the adopted child ran its worker once"


async def test_recovery_crash_after_child_file_adopts_the_same_child(store, tmp_path):
    parent_run = seed_parent(store, tmp_path, "parent", stage="created")
    # A real child record exists but its supervisor never started.
    from polybridge.workflow_invocation import child_run_record, write_child_run
    activation = parent_run["activations"][0]
    child = child_run_record(store, store.get_run(parent_run["workflow_run_id"]), activation, next(n for n in parent_run["definition"]["nodes"] if n["id"] == "call"), activation["invocation"])
    write_child_run(store, child)
    registry = TreeRegistry(store.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(parent_run["workflow_run_id"]), 15)
    final = store.get_run(parent_run["workflow_run_id"])
    assert final["status"] == "completed", final.get("attention_reason")
    children = child_runs(store, final)
    assert len(children) == 1
    assert children[0]["workflow_run_id"] == child["workflow_run_id"]


async def test_recovery_mismatched_child_puts_the_tree_in_needs_attention(store, tmp_path):
    parent_run = seed_parent(store, tmp_path, "parent", stage="created")
    from polybridge.workflow_invocation import child_run_record, write_child_run
    activation = parent_run["activations"][0]
    child = child_run_record(store, store.get_run(parent_run["workflow_run_id"]), activation, next(n for n in parent_run["definition"]["nodes"] if n["id"] == "call"), activation["invocation"])
    child["parent_link"]["workflow_run_id"] = "someone-else"
    write_child_run(store, child)
    registry = TreeRegistry(store.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(parent_run["workflow_run_id"]), 15)
    final = store.get_run(parent_run["workflow_run_id"])
    assert final["status"] == "needs_attention"
    assert invocation_activation(final)["status"] == "uncertain"


async def test_recovery_restart_after_settlement_normalizes_the_outcome(store, tmp_path):
    parent_run = seed_parent(store, tmp_path, "parent", stage="running")
    activation = parent_run["activations"][0]
    child = finished_child(store, parent_run, activation, status="completed", summary="Recovered summary")
    write_run(store, child)
    registry = TreeRegistry(store.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(parent_run["workflow_run_id"]), 15)
    final = store.get_run(parent_run["workflow_run_id"])
    assert final["status"] == "completed", final.get("attention_reason")
    outcome = invocation_activation(final)["node_result"]["result"]["child_outcome"]
    assert outcome["kind"] == "completed"
    assert outcome["summary"] == "Recovered summary"
    assert not [call for call in registry.dispatches if call["label"] == "work"], "the settled child was not re-executed"


def _recover_policy(pick: str):
    """The child fails its first End decision; the parent recovers or retries it."""
    def policy(context, registry):
        if context["current_stage"]["node_id"] == "end" and context["original_request"] == "Focused assignment for call":
            if registry.occurrences.get("child · Decision", 0) <= 3:
                return {"decision_id": context["decision_id"], "action": "failed", "reason": "Child objective failed"}
            return {"decision_id": context["decision_id"], "action": "complete", "reason": "Objective met on retry"}
        if context["original_request"] == "ORIGINAL PARENT REQUEST" and not [c for c in context["valid_continuations"] if c["kind"] != "retry_execution"]:
            reopen = next((c for c in context["valid_continuations"] if c.get("reopen_child")), None)
            retry = next((c for c in context["valid_continuations"] if c["kind"] == "retry_execution" and not c.get("reopen_child")), None)
            chosen = reopen if pick == "recover_child" and reopen else retry if retry else None
            if chosen:
                return {"decision_id": context["decision_id"], "action": "continue", "reason": "Reassign the child", "next": [{"continuation_id": chosen["continuation_id"], "prompt": "Focused assignment for call"}]}
        return workflow_policy(context, registry)
    return policy


async def _run_child_failed_tree(store, tmp_path, pick):
    registry = TreeRegistry(store.root, _recover_policy(pick))
    run, registry, task, supervisor = await _start_tree(store, tmp_path, "parent", registry)
    await asyncio.wait_for(task, 20)
    return store.get_run(run["workflow_run_id"]), registry


async def test_recover_child_reopens_the_failed_child_without_a_new_attempt(store, tmp_path):
    definition = store.get("parent")
    definition["nodes"][1]["max_attempts"] = 1
    store.save("parent", definition, definition["revision"])
    final, registry = await _run_child_failed_tree(store, tmp_path, "recover_child")
    assert final["status"] == "completed", final.get("attention_reason")
    children = child_runs(store, final)
    assert len(children) == 1, "recover_child reuses the same child"
    invocations = [a for a in final["activations"] if a.get("invocation")]
    assert len(invocations) == 1, "recover_child consumes no attempt"
    assert invocations[0]["node_result"]["result"]["child_outcome"]["kind"] == "completed"
    # The child's completed work was preserved: its worker ran exactly once.
    assert len([call for call in registry.dispatches if call["label"] == "work"]) == 1


async def test_retry_creates_a_new_child_and_consumes_an_attempt(store, tmp_path):
    final, registry = await _run_child_failed_tree(store, tmp_path, "retry")
    assert final["status"] == "completed", final.get("attention_reason")
    children = child_runs(store, final)
    assert len(children) == 2, "retry creates a new child invocation"
    invocations = [a for a in final["activations"] if a.get("invocation")]
    assert len(invocations) == 2, "retry consumes a node attempt"
    assert invocations[0]["status"] == "failed"
    assert invocations[1]["node_result"]["result"]["child_outcome"]["kind"] == "completed"


async def test_neither_continuation_is_offered_for_cancelled_or_uncertain_or_permission(store, tmp_path):
    parent_run = store.create_run(w.validate_definition(store.get("parent")), "go", tmp_path, permission_policy="saved_node", dependency_tree=refs.resolve_dependencies(store, definition=store.get("parent")))
    node = next(n for n in parent_run["definition"]["nodes"] if n["id"] == "call")
    token = {"id": "t1", "node_id": "call", "stack": [], "context": {}, "execution_complete": True, "execution_activation_id": "a1", "result": {}}
    for kind in ("cancelled", "uncertain", "permission", "authority"):
        activation = {"id": "a1", "node_id": "call", "role": "node", "status": "failed", "tasks": [], "invocation": {"child_workflow_run_id": "c1", "workflow_id": node["workflow_ref"]["workflow_id"], "stage": "settled"}, "node_result": {"status": "blocked", "result": {"child_outcome": {"kind": kind}, "failure_kind": kind}, "evidence": []}}
        def with_activation(r):
            r["activations"] = [activation]
            r["pending"] = [token]
        store.update_run(parent_run["workflow_run_id"], with_activation, "fixture")
        choices = d.continuations(store.get_run(parent_run["workflow_run_id"]), node, token, False, root=store.root)
        assert not any(c["kind"] == "retry_execution" for c in choices), kind


async def test_child_question_answer_routes_exact_checkpoint_without_repeating_work(store, tmp_path):
    def policy(context, registry):
        if context['workflow_graph']['nodes'][1]['id'] == 'work' and context['current_stage']['node_id'] == 'end' and not context['recovery_instructions']:
            return {'decision_id': context['decision_id'], 'action': 'needs_input', 'question': 'Accept child result?', 'reason': 'Need caller judgment'}
        return workflow_policy(context, registry)

    registry = TreeRegistry(store.root, policy=policy)
    run, _ = await run_tree(store, tmp_path, 'parent', registry)
    assert run['status'] == 'needs_input'
    child = child_runs(store, run)[0]
    decision_id = child['input_decision_id']
    assert run['input_decision_id'] == decision_id
    assert run['input_source']['workflow_run_id'] == child['workflow_run_id']
    assert run['input_question'] == child['input_question']
    with pytest.raises(w.WorkflowError, match='decision_id'):
        store.control(run['workflow_run_id'], 'resume', 'wrong answer', decision_id='stale')
    assert store.get_run(child['workflow_run_id'])['status'] == 'needs_input'
    previous_ids = [a['id'] for a in child['activations'] if a['role'] == 'node']
    store.control(run['workflow_run_id'], 'resume', 'Accepted exact child result', decision_id=decision_id)
    assert store.get_run(child['workflow_run_id'])['instructions'] == 'Accepted exact child result'
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(run['workflow_run_id']), 10)
    final = store.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    after = store.get_run(child['workflow_run_id'])
    assert [a['id'] for a in after['activations'] if a['role'] == 'node'] == previous_ids
    assert len([call for call in registry.dispatches if call['label'] == 'work']) == 1
    assert not final['attempt_grants'] and not after['attempt_grants']


async def test_parallel_questions_preserve_first_identity_and_route_each_answer(store, tmp_path):
    # Use linked runtime-created children, then suspend both at their own durable
    # checkpoints to exercise concurrent publication and the public control seam.
    from test_workflow_run_node_execution import parent_multi
    child = store.get('child')
    store.save('parallel-parent', {**parent_multi([child['workflow_id'], child['workflow_id']]), 'name': 'parallel-parent'})
    run, registry = await run_tree(store, tmp_path, 'parallel-parent')
    children = sorted(child_runs(store, run), key=lambda r: r['created_at'])
    store.update_run(run['workflow_run_id'], lambda r: r.update(status='running'), 'test_reopen_root')
    for index, source in enumerate(children):
        def suspend(r, index=index):
            r.update(status='needs_input', input_question=f'Question {index}', input_decision_id=f'question-{index}')
        store.update_run(source['workflow_run_id'], suspend, 'test_simultaneous_question')
    first, second = [store.get_run(c['workflow_run_id']) for c in children]
    inv.publish_input(store, second)
    inv.publish_input(store, first)
    root = store.get_run(run['workflow_run_id'])
    assert root['input_decision_id'] == first['input_decision_id']
    assert root['input_source']['workflow_run_id'] == first['workflow_run_id']
    with pytest.raises(w.WorkflowError, match='decision_id'):
        store.control(root['workflow_run_id'], 'resume', 'wrong source', decision_id=second['input_decision_id'])
    store.control(root['workflow_run_id'], 'resume', 'First answer', decision_id=first['input_decision_id'])
    assert store.get_run(first['workflow_run_id'])['instructions'] == 'First answer'
    assert store.get_run(second['workflow_run_id'])['status'] == 'needs_input'
    inv.publish_input(store, store.get_run(second['workflow_run_id']))
    root = store.get_run(root['workflow_run_id'])
    assert root['input_decision_id'] == second['input_decision_id']
    store.control(root['workflow_run_id'], 'resume', 'Second answer', decision_id=second['input_decision_id'])
    assert store.get_run(second['workflow_run_id'])['instructions'] == 'Second answer'
    assert store.get_run(first['workflow_run_id'])['instructions'] == 'First answer'
    assert not store.get_run(root['workflow_run_id'])['attempt_grants']
    assert all(not store.get_run(c['workflow_run_id'])['attempt_grants'] for c in children)


@pytest.mark.parametrize("watch_interval", [0.1, 2.0])
async def test_invocation_timeout_cancels_live_child_and_classifies_timeout(store, tmp_path, monkeypatch, watch_interval):
    monkeypatch.setattr(inv, "INVOCATION_WATCH_INTERVAL", watch_interval)
    definition = store.get('parent')
    definition['nodes'][1]['timeout_seconds'] = 1
    store.save('parent', definition, definition['revision'])

    class LiveRegistry(TreeRegistry):
        def __init__(self, root):
            super().__init__(root)
            self.cancelled = []

        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if kwargs.get('title') == 'child · work':
                task.result['status'] = 'running'
                task.done.clear()
            return task

        async def cancel_cascade(self, task_id, **kwargs):
            task = self.tasks.get(task_id)
            if task is not None:
                self.cancelled.append(task_id)
                task.result['status'] = 'cancelled'
                task.done.set()
            return {}

    registry = LiveRegistry(store.root)
    run, _ = await run_tree(store, tmp_path, 'parent', registry, timeout=10)
    activation = invocation_activation(run)
    assert activation['node_result']['result']['child_outcome']['kind'] == 'timeout'
    assert activation['invocation']['timeout_confirmed'] is True
    child = child_runs(store, run)[0]
    assert child['status'] == 'cancelled'
    work = next(a for a in child['activations'] if a['role'] == 'node')
    assert work['tasks'][0]['task_id'] in registry.cancelled
    assert work['tasks'][0]['status'] == 'cancelled'


async def test_cancel_subtree_includes_nested_grandchildren_only(store, tmp_path):
    run, registry = await run_tree(store, tmp_path, 'parent')
    child = child_runs(store, run)[0]
    grandchild = copy.deepcopy(child)
    grandchild['workflow_run_id'] = uuid.uuid4().hex
    grandchild['parent_link'].update(workflow_run_id=child['workflow_run_id'], execution_id='nested')
    grandchild['status'] = 'running'
    grandchild['activations'] = []
    write_run(store, grandchild)
    store.update_run(child['workflow_run_id'], lambda r: r.update(status='running', activations=[{'id': 'nested', 'tasks': [], 'invocation': {'child_workflow_run_id': grandchild['workflow_run_id']}}]), 'test_live_child')
    await inv.WorkflowTree(store).cancel_descendants(child['workflow_run_id'], registry, include_self=True)
    assert store.get_run(child['workflow_run_id'])['status'] == 'cancelled'
    assert store.get_run(grandchild['workflow_run_id'])['status'] == 'cancelled'
    assert store.get_run(run['workflow_run_id'])['status'] == 'completed'


def test_final_refs_preserve_all_end_inputs_and_recursive_permission_evidence():
    executions = [{'id': str(i), 'node_id': 'work', 'role': 'node', 'status': 'completed', 'tasks': [], 'node_result': {'status': 'succeeded', 'result': {}}} for i in range(60)]
    end = {'id': 'end-turn', 'token': {'input_result_refs': [str(i) for i in range(1, 60)]}}
    child = {'workflow_run_id': 'child', 'definition': {'nodes': [{'id': 'end', 'type': 'end'}]}, 'activations': executions + [end], 'decisions': [{'action': 'complete', 'node_id': 'end', 'activation_id': 'end-turn'}]}
    assert [r['execution_id'] for r in inv.final_result_refs(child)] == [str(i) for i in range(1, 60)]
    denials = [{'reason': str(i)} for i in range(60)]
    child['activations'][0]['node_result']['result']['child_outcome'] = {'permission_evidence': denials}
    assert inv.collect_permission_evidence(child) == denials


@pytest.mark.parametrize('action', ['retry', 'recover_child'])
async def test_reconcile_preserves_accepted_child_retry_or_recovery_token(store, tmp_path, action):
    parent = seed_parent(store, tmp_path, 'parent', stage='created')
    activation = parent['activations'][0]
    child = finished_child(store, parent, activation, status='failed', failed_decision='failed-checkpoint')
    write_run(store, child)
    def accepted(r):
        old = r['activations'][0]
        old.update(status='failed', node_result=inv.invocation_outcome('child_failed', child, old['invocation'], r['definition']['nodes'][1]))
        token = r['pending'][0]
        token['assignment_prompt'] = 'Explicit follow-up after failed child'
        if action == 'retry':
            token.pop('execution_activation_id', None)
            token['retry_of_execution_id'] = old['id']
        else:
            token['reopen_child_execution_id'] = old['id']
    store.update_run(parent['workflow_run_id'], accepted, 'test_accepted_followup')
    reconciled = store.reconcile_run(parent['workflow_run_id'])
    token = reconciled['pending'][0]
    assert not token.get('execution_complete')
    if action == 'retry':
        assert token['retry_of_execution_id'] == activation['id']
        assert 'execution_activation_id' not in token
    else:
        assert token['reopen_child_execution_id'] == activation['id']
        assert token['execution_activation_id'] == activation['id']


async def test_root_resume_routes_child_attention_grants_without_repeating_work(store, tmp_path):
    definition = store.get('child')
    definition['max_transitions'] = 1
    store.save('child', definition, expected_revision=definition['revision'])
    registry = TreeRegistry(store.root)
    run, _ = await run_tree(store, tmp_path, 'parent', registry)
    assert run['status'] == 'needs_attention'
    child = child_runs(store, run)[0]
    assert run['attention_source']['workflow_run_id'] == child['workflow_run_id']
    assert child['status'] == 'needs_attention'
    old_child_id = child['workflow_run_id']
    old_completed = [a['id'] for a in child['activations'] if a.get('role') == 'node' and a.get('status') == 'completed']
    store.control(run['workflow_run_id'], 'resume', 'Continue the child', additional_attempts=10)
    resumed = store.get_run(old_child_id)
    assert resumed['status'] == 'running'
    assert resumed['instructions'] == 'Continue the child'
    assert resumed['transition_grant'] == 10
    assert not store.get_run(run['workflow_run_id']).get('transition_grant')
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(run['workflow_run_id']), 10)
    final = store.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    assert [c['workflow_run_id'] for c in child_runs(store, final)] == [old_child_id]
    assert len([call for call in registry.dispatches if call['label'] == 'work']) == 1
    assert all(any(a['id'] == ident for a in store.get_run(old_child_id)['activations']) for ident in old_completed)


async def test_child_between_turns_settles_when_root_failed(store, tmp_path):
    parent = seed_parent(store, tmp_path, 'parent', stage='created')
    activation = parent['activations'][0]
    child = finished_child(store, parent, activation, status='running')
    write_run(store, child)
    store.update_run(parent['workflow_run_id'], lambda r: r.update(status='failed'), 'fixture')
    registry = TreeRegistry(store.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, store).execute(child['workflow_run_id']), 3)
    assert store.get_run(child['workflow_run_id'])['status'] == 'cancelled'
    assert not registry.dispatches


def test_forwarded_paused_child_resume_grants_only_source(store, tmp_path):
    parent = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = finished_child(store, parent, parent['activations'][0], status='paused')
    write_run(store, child)
    source = {'workflow_run_id': child['workflow_run_id'], 'workflow_name': child['name']}
    store.update_run(parent['workflow_run_id'], lambda r: r.update(status='paused', attention_source=source), 'fixture')
    store.control(parent['workflow_run_id'], 'resume', 'Resume child only', additional_attempts=2)
    assert store.get_run(child['workflow_run_id'])['status'] == 'running'
    assert store.get_run(child['workflow_run_id'])['transition_grant'] == 2
    assert not store.get_run(parent['workflow_run_id']).get('transition_grant')
    assert store.get_run(parent['workflow_run_id'])['status'] == 'running'


async def test_cancellation_does_not_settle_parent_with_uncertain_grandchild(store, tmp_path):
    parent = seed_parent(store, tmp_path, 'parent', stage='running')
    activation = parent['activations'][0]
    child = finished_child(store, parent, activation, status='running')
    grandchild_id = uuid.uuid4().hex
    child['activations'] = [{'id': 'nested', 'node_id': 'inner-call', 'role': 'node', 'status': 'running', 'tasks': [], 'invocation': {'child_workflow_run_id': grandchild_id, 'stage': 'running'}}]
    write_run(store, child)
    grandchild = copy.deepcopy(child)
    grandchild['workflow_run_id'] = grandchild_id
    grandchild['parent_link'].update(workflow_run_id=child['workflow_run_id'], execution_id='nested')
    grandchild['activations'] = [{'id': 'live', 'node_id': 'work', 'role': 'node', 'status': 'running', 'tasks': [{'task_id': 'uncertain-live', 'status': 'uncertain'}]}]
    write_run(store, grandchild)

    class RefusedCancel:
        async def cancel_cascade(self, *args, **kwargs):
            raise RuntimeError('Cannot prove process identity')

    await inv.WorkflowTree(store).cancel_descendants(parent['workflow_run_id'], RefusedCancel())
    updated = store.get_run(child['workflow_run_id'])
    assert updated['status'] == 'cancelling'
    assert not inv.child_settled(updated, store=store)
    assert store.get_run(grandchild_id)['status'] == 'cancelling'
    # Even a premature historical terminal mark cannot pass settlement proof.
    store.update_run(child['workflow_run_id'], lambda r: r.update(status='cancelled'), 'test_premature_terminal')
    outcome = inv.outcome_for_child(store, parent, activation, parent['definition']['nodes'][1])
    assert outcome['result']['child_outcome']['kind'] == 'uncertain'
    reconciled = store.reconcile_run(parent['workflow_run_id'])
    assert not reconciled['pending'][0].get('execution_complete')
    assert reconciled['status'] == 'needs_attention'
    assert reconciled['activations'][0]['status'] == 'uncertain'


async def test_cancel_linked_tree_never_reads_unrelated_giant_history(store, tmp_path, monkeypatch):
    parent = seed_parent(store, tmp_path, 'parent', stage='running')
    child = finished_child(store, parent, parent['activations'][0], status='running')
    child['activations'] = []
    write_run(store, child)
    unrelated = store.runs / 'unrelated-giant.json'
    unrelated.write_bytes(b'x' * (5 * 1024 * 1024))
    monkeypatch.setattr(store, 'list_runs', lambda *a, **k: pytest.fail('cancel scanned retained history'))
    read = store.get_run
    ids = []
    def only_linked(identifier, **kwargs):
        ids.append(identifier)
        assert identifier in {parent['workflow_run_id'], child['workflow_run_id']}
        return read(identifier, **kwargs)
    monkeypatch.setattr(store, 'get_run', only_linked)
    await inv.WorkflowTree(store).cancel_descendants(parent['workflow_run_id'], object(), include_self=True)
    assert read(parent['workflow_run_id'])['status'] == 'cancelled'
    assert read(child['workflow_run_id'])['status'] == 'cancelled'
    assert len(unrelated.read_bytes()) == 5 * 1024 * 1024


@pytest.mark.parametrize('kind', ['missing', 'corrupt', 'mismatched', 'cycle'])
async def test_cancel_unprovable_child_link_keeps_source_unsettled(store, tmp_path, kind):
    parent = seed_parent(store, tmp_path, 'parent', stage='running')
    activation = parent['activations'][0]
    child_id = activation['invocation']['child_workflow_run_id']
    if kind == 'corrupt':
        (store.runs / f'{child_id}.json').write_text('{broken')
    elif kind == 'mismatched':
        child = finished_child(store, parent, activation, status='running')
        child['parent_link']['execution_id'] = 'foreign-execution'
        write_run(store, child)
    elif kind == 'cycle':
        child = finished_child(store, parent, activation, status='running')
        child['activations'] = [{'id': 'back', 'tasks': [], 'invocation': {'child_workflow_run_id': parent['workflow_run_id']}}]
        write_run(store, child)
        store.update_run(parent['workflow_run_id'], lambda r: r.update(parent_link={'workflow_run_id': child_id, 'execution_id': 'back'}), 'cycle_fixture')
    await inv.WorkflowTree(store).cancel_descendants(parent['workflow_run_id'], object(), include_self=True)
    assert store.get_run(parent['workflow_run_id'])['status'] == 'cancelling'
    if kind == 'mismatched':
        assert store.get_run(child_id)['status'] == 'running'
    if kind == 'cycle':
        assert store.get_run(child_id)['status'] == 'cancelling'


async def test_cancel_duplicate_invocation_and_completed_child_is_idempotent(store, tmp_path):
    parent = seed_parent(store, tmp_path, 'parent', stage='running')
    activation = parent['activations'][0]
    child = finished_child(store, parent, activation, status='completed')
    write_run(store, child)
    store.update_run(parent['workflow_run_id'], lambda r: r['activations'].append(copy.deepcopy(r['activations'][0])), 'duplicate_fixture')
    class NoCancel:
        async def cancel_cascade(self, *a, **k):
            pytest.fail('completed child task was cancelled')
    await inv.WorkflowTree(store).cancel_descendants(parent['workflow_run_id'], NoCancel(), include_self=True)
    assert store.get_run(parent['workflow_run_id'])['status'] == 'cancelled'
    assert store.get_run(child['workflow_run_id'])['status'] == 'completed'


@pytest.mark.parametrize('source', ['root_cancel', 'root_failure', 'invocation_timeout'])
async def test_terminal_parallel_child_live_tasks_are_signaled_without_overwriting_failure(store, tmp_path, source):
    parent = seed_parent(store, tmp_path, 'parent', stage='running')
    activation = parent['activations'][0]
    child = finished_child(store, parent, activation, status='failed')
    child['failure_reason'] = 'One parallel branch failed'
    child['definition'] = child_graph('child', parallel=True, work_count=2)
    child['activations'] = [
        {'id': 'settled', 'role': 'node', 'node_id': 'work1', 'status': 'failed', 'tasks': [{'task_id': 'completed-work', 'status': 'completed'}, {'task_id': 'failed-work', 'status': 'failed'}]},
        {'id': 'sibling', 'role': 'node', 'node_id': 'work2', 'status': 'running', 'tasks': [{'task_id': 'running-work', 'status': 'running'}, {'task_id': 'reserved-work', 'status': 'reserved'}, {'task_id': 'uncertain-work', 'status': 'uncertain'}]},
    ]
    write_run(store, child)
    if source != 'invocation_timeout':
        store.update_run(parent['workflow_run_id'], lambda r: r.update(status='cancelling' if source == 'root_cancel' else 'failed'), 'root_fixture')
    class RefusedCancel:
        def __init__(self):
            self.calls = []
        async def cancel_cascade(self, task_id, **kwargs):
            self.calls.append((task_id, kwargs))
            raise RuntimeError('Task identity remains uncertain')
    registry = RefusedCancel()
    target = child['workflow_run_id'] if source == 'invocation_timeout' else parent['workflow_run_id']
    await inv.WorkflowTree(store).cancel_descendants(target, registry, include_self=source == 'invocation_timeout')
    assert [task_id for task_id, _ in registry.calls] == ['running-work', 'reserved-work', 'uncertain-work']
    assert all(options['workflow_control'] for _, options in registry.calls)
    updated = store.get_run(child['workflow_run_id'])
    assert updated['status'] == 'failed' and updated['failure_reason'] == 'One parallel branch failed'
    assert not inv.child_settled(updated, store=store)
    if source == 'invocation_timeout':
        activation['invocation'].update(timeout_expired=True, timeout_confirmed=False)
        outcome = inv.outcome_for_child(store, parent, activation, parent['definition']['nodes'][1])
        assert outcome['result']['child_outcome']['kind'] == 'uncertain'
        assert store.get_run(parent['workflow_run_id'])['status'] == 'running'


@pytest.mark.parametrize('kind', ['input', 'paused', 'failed'])
@pytest.mark.parametrize('failure_target', ['child', 'child-after-write', 'parent'])
def test_forwarded_resume_delivery_is_durable_and_exactly_once(store, tmp_path, monkeypatch, kind, failure_target):
    parent = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = finished_child(store, parent, parent['activations'][0], status='needs_input' if kind == 'input' else kind)
    child.update(interaction_owner='monitor')
    source = {'workflow_run_id': child['workflow_run_id'], 'workflow_name': child['name']}
    if kind == 'input':
        child.update(input_decision_id='question-one', input_question='Answer me')
    write_run(store, child)
    def seed(r):
        r.update(interaction_owner='monitor', status='needs_input' if kind == 'input' else 'needs_attention')
        if kind == 'input':
            r.update(input_source=source, input_decision_id='question-one', input_question='Answer me')
        else:
            r.update(attention_source=source)
    store.update_run(parent['workflow_run_id'], seed, 'fixture')
    original = store.update_run
    failed = False
    def injected(identifier, change, event, detail=None):
        nonlocal failed
        target = identifier == child['workflow_run_id'] and event in {'control:resume', 'control:recover'} if failure_target.startswith('child') else identifier == parent['workflow_run_id'] and event in {'forwarded_input_answered', 'forwarded_attention_resumed'}
        if target and not failed:
            failed = True
            if failure_target == 'child-after-write':
                original(identifier, change, event, detail)
            raise OSError('injected write failure')
        return original(identifier, change, event, detail)
    monkeypatch.setattr(store, 'update_run', injected)
    options = {'decision_id': 'question-one'} if kind == 'input' else {}
    with pytest.raises(OSError, match='injected'):
        store.control(parent['workflow_run_id'], 'resume', 'exact answer', 2, interaction_owner='monitor', **options)
    # Restart: the delivery identity and accepted child receipt come from disk.
    restarted = w.WorkflowStore(root=store.root)
    with pytest.raises(w.WorkflowError, match='original caller'):
        restarted.control(parent['workflow_run_id'], 'resume', 'exact answer', 2, interaction_owner='caller', **options)
    with pytest.raises(w.WorkflowError, match='pending'):
        restarted.control(parent['workflow_run_id'], 'resume', 'changed answer', 3, interaction_owner='monitor', **options)
    if failure_target == 'parent':
        # A child may already publish its next checkpoint before parent acknowledgment.
        restarted.update_run(child['workflow_run_id'], lambda r: r.update(status='needs_input', input_decision_id='question-two', input_question='Next question'), 'fixture')
    result = restarted.control(parent['workflow_run_id'], 'resume', 'exact answer', 2, interaction_owner='monitor', **options)
    assert result['status'] == 'running' and 'forwarded_delivery' not in result
    actual = restarted.get_run(child['workflow_run_id'])
    assert actual['transition_grant'] == 2
    assert actual['instructions'] == 'exact answer'
    assert not result.get('transition_grant')
    if failure_target == 'parent':
        assert actual['input_decision_id'] == 'question-two' and actual['status'] == 'needs_input'
    else:
        assert actual['status'] == 'running'


def test_forwarded_delivery_retry_preserves_and_allows_new_parent_question(store, tmp_path, monkeypatch):
    parent = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = finished_child(store, parent, parent['activations'][0], status='needs_input')
    source = {'workflow_run_id': child['workflow_run_id'], 'workflow_name': child['name']}
    child.update(input_decision_id='one', input_question='First')
    write_run(store, child)
    store.update_run(parent['workflow_run_id'], lambda r: r.update(status='needs_input', input_source=source, input_decision_id='one', input_question='First'), 'fixture')
    original = store.update_run
    def moved(identifier, change, event, detail=None):
        if event == 'forwarded_input_answered':
            original(child['workflow_run_id'], lambda r: r.update(status='needs_input', input_decision_id='two', input_question='Second'), 'fixture')
            original(parent['workflow_run_id'], lambda r: r.update(input_decision_id='two', input_question='Second'), 'fixture')
        return original(identifier, change, event, detail)
    monkeypatch.setattr(store, 'update_run', moved)
    with pytest.raises(w.WorkflowError, match='moved on'):
        store.control(parent['workflow_run_id'], 'resume', 'First answer', 2, decision_id='one')
    restarted = w.WorkflowStore(root=store.root)
    with pytest.raises(w.WorkflowError, match='current input decision_id'):
        restarted.control(parent['workflow_run_id'], 'resume', 'First answer', 2, decision_id='one')
    assert restarted.get_run(parent['workflow_run_id'])['input_question'] == 'Second'
    restarted.control(parent['workflow_run_id'], 'resume', 'Second answer', 1, decision_id='two')
    assert restarted.get_run(child['workflow_run_id'])['transition_grant'] == 3
    assert restarted.get_run(child['workflow_run_id'])['instructions'] == 'Second answer'


def test_forwarded_attention_new_source_uses_its_own_control_action(store, tmp_path, monkeypatch):
    parent = seed_parent(store, tmp_path, 'parent', stage='waiting')
    old_child = finished_child(store, parent, parent['activations'][0], status='failed')
    new_child = copy.deepcopy(old_child)
    new_child.update(workflow_run_id=uuid.uuid4().hex, status='paused')
    write_run(store, old_child)
    write_run(store, new_child)
    old_source = {'workflow_run_id': old_child['workflow_run_id'], 'workflow_name': 'child'}
    new_source = {'workflow_run_id': new_child['workflow_run_id'], 'workflow_name': 'child'}
    store.update_run(parent['workflow_run_id'], lambda r: r.update(status='needs_attention', attention_source=old_source), 'fixture')
    original = store.update_run
    def moved(identifier, change, event, detail=None):
        if event == 'forwarded_attention_resumed':
            original(parent['workflow_run_id'], lambda r: r.update(attention_source=new_source), 'fixture')
        return original(identifier, change, event, detail)
    monkeypatch.setattr(store, 'update_run', moved)
    with pytest.raises(w.WorkflowError, match='moved on'):
        store.control(parent['workflow_run_id'], 'resume', 'Recover old child', 2)
    assert store.get_run(old_child['workflow_run_id'])['transition_grant'] == 2
    restarted = w.WorkflowStore(root=store.root)
    result = restarted.control(parent['workflow_run_id'], 'resume', 'Resume new child', 1)
    assert result['status'] == 'running'
    assert restarted.get_run(new_child['workflow_run_id'])['status'] == 'running'
    assert restarted.get_run(new_child['workflow_run_id'])['transition_grant'] == 1
    assert restarted.get_run(old_child['workflow_run_id'])['transition_grant'] == 2


def test_forwarded_attention_same_child_new_checkpoint_survives_prior_ack(store, tmp_path, monkeypatch):
    parent = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = finished_child(store, parent, parent['activations'][0], status='paused')
    write_run(store, child)
    cid = child['workflow_run_id']
    first = store.update_run(cid, lambda r: r.update(attention_reason='Same reason'), 'suspend')
    inv.publish_attention(store, first)
    first_source = store.get_run(parent['workflow_run_id'])['attention_source']
    # Repeated publications and unrelated writes retain this suspension identity.
    inv.publish_attention(store, store.update_run(cid, lambda r: r.update(instructions='unchanged suspension'), 'poll'))
    assert store.get_run(parent['workflow_run_id'])['attention_source'] == first_source
    original = store.update_run
    def moved(identifier, change, event, detail=None):
        if event == 'forwarded_attention_resumed':
            again = original(cid, lambda r: r.update(status='paused', attention_reason='Same reason'), 'suspend_again')
            inv.publish_attention(store, again)
        return original(identifier, change, event, detail)
    monkeypatch.setattr(store, 'update_run', moved)
    with pytest.raises(w.WorkflowError, match='moved on'):
        store.control(parent['workflow_run_id'], 'resume', 'First answer', 2)
    suspended = store.get_run(parent['workflow_run_id'])
    assert suspended['status'] == 'paused'
    assert suspended['attention_source']['attention_checkpoint'] != first_source['attention_checkpoint']
    restarted = w.WorkflowStore(root=store.root)
    result = restarted.control(parent['workflow_run_id'], 'resume', 'Second answer', 1)
    assert result['status'] == 'running'
    actual = restarted.get_run(cid)
    assert actual['transition_grant'] == 3
    assert actual['instructions'] == 'Second answer'
