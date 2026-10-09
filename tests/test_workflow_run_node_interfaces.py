"""Run workflow node interfaces: previews, scoped inspection, gates and authority."""
import asyncio
import copy
import json
import uuid
from types import SimpleNamespace

import pytest

from polybridge import backends, control, store as task_store
from polybridge import workflow_delegation as d
from polybridge import workflow_hooks
from polybridge import workflow_inspection
from polybridge import workflow_invocation as inv
from polybridge import workflow_references as refs
from polybridge import workflows as w

from test_workflow_run_node_execution import TreeRegistry, _start_tree, _wait_until, child_graph, child_runs, parent_graph, run_tree
from test_workflow_run_node_recovery import finished_child, seed_parent, write_run


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    s = w.WorkflowStore(tmp_path)
    child = s.save("child", child_graph("child"))
    s.save("parent", parent_graph(child["workflow_id"]))
    return s


def seeded_two_level(store, tmp_path, *, big_result=None):
    """A parent whose workflow invocation settled with a (large) child result."""
    parent_run = seed_parent(store, tmp_path, "parent", stage="running")
    activation = parent_run["activations"][0]
    child = finished_child(store, parent_run, activation, status="completed", summary="Child finished", executions=[("work", "succeeded", {"summary": "Child did the work"})])
    if big_result is not None:
        child["activations"].append({"id": "big", "node_id": "work", "role": "node", "status": "completed", "tasks": [{"task_id": "big-task", "status": "completed", "result": {"status": "completed", "summary": big_result}}], "created_at": 0.0, "node_result": {"status": "succeeded", "result": {"summary": big_result}, "evidence": []}, "token": {"id": "big-token"}})
    write_run(store, child)
    return parent_run, activation, child


def settled_parent(store, parent_run, activation, child):
    """The parent record with the invocation settled and a decision checkpoint."""
    run = copy.deepcopy(store.get_run(parent_run["workflow_run_id"]))
    run["activations"][0]["status"] = "completed"
    run["activations"][0]["node_result"] = {"status": "succeeded", "result": {"child_outcome": {"kind": "completed", "child_workflow_run_id": child["workflow_run_id"], "workflow_id": activation["invocation"]["workflow_id"], "final_result_refs": [{"workflow_run_id": child["workflow_run_id"], "execution_id": "big", "via": [child["workflow_run_id"]]}]}}, "evidence": []}
    token = run["pending"][0]
    token.update(execution_complete=True, execution_activation_id=activation["id"], decision_id=uuid.uuid4().hex, decision_attempts=0)
    return run, token


async def test_orchestrator_preview_is_bounded_with_leaf_refs_and_inspect_hint(store, tmp_path):
    big = "C-RESULT " * 6000  # Well beyond the 16k per-ref preview limit.
    parent_run, activation, child = seeded_two_level(store, tmp_path, big_result=big)
    settled, token = settled_parent(store, parent_run, activation, child)
    context = d.decision_context(settled, next(n for n in settled["definition"]["nodes"] if n["id"] == "call"), token, False, root=store.root)
    inputs = context["input_results"]
    assert inputs and inputs[0].get("child_outcome")
    previews = inputs[0]["child_result_previews"]
    assert previews
    budget = sum(len(p.get("result_preview", "")) for p in previews)
    assert budget <= inv.INVOCATION_PREVIEW_BUDGET
    assert all(len(p.get("result_preview", "")) <= inv.INVOCATION_PREVIEW_REF_LIMIT for p in previews)
    assert any(p.get("truncated") for p in previews)
    assert previews[0]["result_ref"]["workflow_run_id"] == child["workflow_run_id"]


async def test_worker_inputs_stay_complete_beyond_the_budget(store, tmp_path):
    big = "C-RESULT " * 6000
    parent_run, activation, child = seeded_two_level(store, tmp_path, big_result=big)
    settled, token = settled_parent(store, parent_run, activation, child)
    complete = d.result_inputs(settled, [activation["id"]], preview=False, root=store.root)
    assert "child_result_previews" not in complete[0]
    assert complete[0]["node_result"]["result"]["child_outcome"]["final_result_refs"]
    assert complete[0]["child_results"][0]["node_result"]["result"]["summary"] == big


async def test_scoped_inspection_authorizes_only_verified_parent_chains(store, tmp_path):
    parent_run, activation, child = seeded_two_level(store, tmp_path)
    # The child's settled execution is inspectable through the verified chain.
    page = workflow_inspection.inspect_request(store.get_run(parent_run["workflow_run_id"]), store.root, {"execution_id": child["activations"][0]["id"], "workflow_run_id": child["workflow_run_id"], "view": "result"})
    assert page["workflow_run_id"] == child["workflow_run_id"]
    # A forged chain is refused: the persisted invocation must name the child.
    with pytest.raises(ValueError, match="Unknown linked child|persisted invocation"):
        workflow_inspection.inspect_request(store.get_run(parent_run["workflow_run_id"]), store.root, {"execution_id": "whatever", "workflow_run_id": "not-a-child", "view": "result"})
    # An unrelated settled run is refused even with a matching execution id.
    other = seed_parent(store, tmp_path, "parent", stage="preparing")
    with pytest.raises(ValueError):
        workflow_inspection.inspect_request(store.get_run(parent_run["workflow_run_id"]), store.root, {"execution_id": "x", "workflow_run_id": other["workflow_run_id"], "view": "result"})


async def test_takeover_pause_and_direct_message_use_the_tree_gate(store, tmp_path):
    parent_run, activation, child = seeded_two_level(store, tmp_path)
    child["activations"].append({"id": "orch", "node_id": "work", "role": "orchestrator", "status": "completed", "tasks": [{"task_id": "kid", "status": "completed", "result": {"status": "completed"}}], "created_at": 0.0, "token": {}})
    child["status"] = "running"
    write_run(store, child)
    receipt = store.owners / "kid.json"
    w._write(receipt, {"workflow_run_id": child["workflow_run_id"]})
    # The root is still active: takeover and direct messages are refused.
    with pytest.raises(control.TakeoverRefused):
        workflow_hooks.refuse_takeover(store.root / "tasks", "kid")
    from polybridge.inbox import SendRefused
    with pytest.raises(SendRefused):
        workflow_hooks.refuse_direct_message(store.root / "tasks", "kid")
    # Pausing a child task pauses the root, not the child.
    workflow_hooks.pause_for_task(store.root / "tasks", "kid", "hold")
    assert store.get_run(parent_run["workflow_run_id"])["status"] == "paused"
    with pytest.raises(w.WorkflowError, match=parent_run["workflow_run_id"]):
        store.control(child["workflow_run_id"], "pause")


async def test_retention_pins_tasks_of_every_run_in_an_active_tree(store, tmp_path):
    parent_run, activation, child = seeded_two_level(store, tmp_path)
    child["activations"].append({"id": "orch", "node_id": "work", "role": "orchestrator", "status": "completed", "tasks": [{"task_id": "pinned-kid", "status": "completed", "result": {"status": "completed"}}], "created_at": 0.0, "token": {}})
    write_run(store, child)
    w._write(store.owners / "pinned-kid.json", {"workflow_run_id": child["workflow_run_id"]})
    assert "pinned-kid" in store.pinned_tasks()
    store.update_run(parent_run["workflow_run_id"], lambda r: r.update(status="completed"), "fixture")
    assert "pinned-kid" not in store.pinned_tasks()


async def test_decorate_tasks_reports_tree_fields_and_session_owner(store, tmp_path):
    parent_run, activation, child = seeded_two_level(store, tmp_path)
    child["orchestrator_mode"] = "current"
    child["orchestrator_session_owner_run_id"] = parent_run["workflow_run_id"]
    child["activations"].append({"id": "orch", "node_id": "work", "role": "orchestrator", "status": "completed", "tasks": [{"task_id": "owned-kid", "status": "completed", "result": {"status": "completed"}}], "created_at": 0.0, "token": {}})
    write_run(store, child)
    w._write(store.owners / "owned-kid.json", {"workflow_run_id": child["workflow_run_id"]})
    decorated = workflow_inspection.decorate_tasks([{"task_id": "owned-kid"}], store.root / "tasks")[0]
    assert decorated["root_workflow_run_id"] == parent_run["workflow_run_id"]
    assert decorated["workflow_session_owner_run_id"] == parent_run["workflow_run_id"]
    assert decorated["workflow_tree_settling"] is True


async def test_compact_projection_carries_tree_fields(store, tmp_path):
    from polybridge.workflow_responses import compact
    parent_run, activation, child = seeded_two_level(store, tmp_path)
    child_run_record = store.get_run(child["workflow_run_id"])
    projection = compact(child_run_record)
    assert projection["parent_workflow_run_id"] == parent_run["workflow_run_id"]
    assert projection["root_workflow_run_id"] == parent_run["workflow_run_id"]
    assert projection["parent_execution_id"] == activation["id"]
    assert projection["orchestrator_mode"] == "child"
    assert projection["tree_settling"] is False  # a settled child reports a settled tree projection
    root_projection = compact(store.get_run(parent_run["workflow_run_id"]))
    assert root_projection["child_invocations"][0]["child_workflow_run_id"] == child["workflow_run_id"]


def _caller(freedom: str, *, network: bool = True):
    enforcement = backends.get("codex").enforcement(freedom, network).as_dict()
    record = SimpleNamespace(task_id="caller-1", backend="codex", session_id=None, repo_path="/tmp", freedom=freedom, network=network, status="completed", enforcement=enforcement)
    return SimpleNamespace(record=record)


async def test_agent_cannot_save_access_beyond_its_envelope_over_the_tree(store, tmp_path):
    rich = child_graph("rich")
    rich["nodes"][1]["freedom"] = "unrestricted"
    saved = store.save("rich", rich)
    parent = parent_graph(saved["workflow_id"], name="envelope")
    tree = refs.resolve_dependencies(store, definition=parent)
    with pytest.raises(ValueError, match="freedom"):
        workflow_inspection.guard_saved_workflow_authority(_caller("write_in_repo"), tree)
    # Inside the envelope it passes.
    workflow_inspection.guard_saved_workflow_authority(_caller("unrestricted"), tree)


async def test_child_mode_orchestrator_is_checked_and_current_mode_is_not(store, tmp_path):
    child_id = store.get("child")["workflow_id"]
    child_mode = refs.resolve_dependencies(store, definition=parent_graph(child_id, mode="child", name="cm"))
    current_mode = refs.resolve_dependencies(store, definition=parent_graph(child_id, mode="current", name="x"))
    # Both trees are inside a read_only envelope except the pinned write nodes;
    # an orchestrator reached through a Child edge must stay read_only-capable.
    workflow_inspection.guard_saved_workflow_authority(_caller("unrestricted"), child_mode)
    workflow_inspection.guard_saved_workflow_authority(_caller("unrestricted"), current_mode)


async def test_start_refuses_before_any_run_is_created(store, tmp_path):
    store.delete("child")
    definition = store.get("parent")
    with pytest.raises(refs.DependencyError, match="Unknown workflow reference"):
        refs.resolve_dependencies(store, definition=definition)
    assert not list((store.runs).glob("*.json"))


async def test_decisions_cannot_set_access_fields(store, tmp_path):
    parent_run, activation, child = seeded_two_level(store, tmp_path)
    run = store.get_run(parent_run["workflow_run_id"])
    token = run["pending"][0]
    token.update(decision_id="d1", decision_attempts=0)
    for field, value in (("freedom", "unrestricted"), ("network", True), ("backend", "claude")):
        decision = {"decision_id": "d1", "action": "continue", "reason": "go", "next": [{"continuation_id": "done", field: value}]}
        with pytest.raises(w.WorkflowError, match="cannot override saved workflow permissions|Unexpected decision fields"):
            d.normalize_decision(run, next(n for n in run["definition"]["nodes"] if n["id"] == "call"), token, decision, False, root=store.root)


def test_inspection_of_grandchild_returns_requested_boundary(store, tmp_path):
    middle = store.save("middle", parent_graph(store.get("child")["workflow_id"], name="middle"))
    store.save("outer", parent_graph(middle["workflow_id"], name="outer"))
    outer = seed_parent(store, tmp_path, "outer", stage="running")
    outer_activation = outer["activations"][0]
    middle_run = inv.child_run_record(store, outer, outer_activation, outer["definition"]["nodes"][1], outer_activation["invocation"])
    middle_run["status"] = "completed"
    leaf_id = uuid.uuid4().hex
    middle_activation = {"id": "middle-call", "node_id": "call", "role": "node", "status": "completed", "tasks": [], "invocation": {"child_workflow_run_id": leaf_id, "workflow_id": store.get("child")["workflow_id"]}}
    middle_run["activations"] = [middle_activation]
    write_run(store, middle_run)
    leaf = inv.child_run_record(store, middle_run, middle_activation, middle_run["definition"]["nodes"][1], middle_activation["invocation"])
    leaf["status"] = "completed"
    leaf["activations"] = [{"id": "leaf-work", "node_id": "work", "role": "node", "status": "completed", "tasks": [], "node_result": {"status": "succeeded", "result": {"summary": "grandchild evidence"}, "evidence": []}}]
    write_run(store, leaf)
    page = workflow_inspection.inspect_request(outer, store.root, {"execution_id": "leaf-work", "workflow_run_id": leaf_id, "view": "result"})
    assert page["workflow_run_id"] == leaf_id
    assert "grandchild evidence" in json.dumps(page)


@pytest.mark.parametrize('delivery', ['legacy', 'optimized_v1'])
async def test_successor_worker_receives_complete_child_result_inline_or_by_reference(store, tmp_path, delivery):
    definition = store.get('parent')
    definition['context_delivery'] = delivery
    definition['nodes'].insert(-1, {'id': 'consume', 'type': 'agent', 'role': 'task', 'instructions': 'Consume predecessor evidence', 'execution_mode': 'headless', 'agent': {'backend': 'codex'}})
    definition['connections'][-1]['target'] = 'consume'
    definition['connections'].append({'id': 'consumed', 'source': 'consume', 'target': 'end'})
    store.save('parent', definition, definition['revision'])
    big = 'Complete child evidence ' * 4000
    class ReadingRegistry(TreeRegistry):
        retrieved = False
        async def start(self, prompt, repo, **kwargs):
            if kwargs.get('title', '').endswith(' · consume'):
                current = next(r for r in store.list_runs() if r['name'] == 'parent' and r['status'] == 'running')
                activation = next(a for a in current['activations'] if a['role'] == 'node' and a['node_id'] == 'consume')
                leaf = next(ref for ref in activation['authorized_input_refs'] if ref['workflow_run_id'] != current['workflow_run_id'])
                chunks, cursor = [], None
                while True:
                    page = workflow_inspection.assigned_input_page(({'role': 'node', 'activation_id': activation['id']}, current), store.root, current['workflow_run_id'], leaf['execution_id'], source_run_id=leaf['workflow_run_id'], cursor=cursor)
                    assert page['content_sha256'] == leaf['content_sha256']
                    chunks.append(page['chunk'])
                    cursor = page['next_cursor']
                    if cursor is None:
                        break
                assert json.loads(''.join(chunks))['node_result']['result']['summary'] == big
                self.retrieved = True
            return await super().start(prompt, repo, **kwargs)
    registry = ReadingRegistry(store.root, outputs={'work': {'status': 'succeeded', 'result': {'summary': big}, 'evidence': []}})
    run, _ = await run_tree(store, tmp_path, 'parent', registry)
    assert run['status'] == 'completed', run.get('attention_reason')
    prompt = next(call['prompt'] for call in registry.dispatches if call['label'] == 'consume')
    assert run['definition']['context_delivery'] == 'optimized_v1'
    assert big not in prompt and registry.retrieved
    assert 'child_retrieval' in prompt
    assert len(big) > inv.INVOCATION_PREVIEW_BUDGET


async def test_final_json_inspection_preserves_and_authorizes_descendant_target(store, tmp_path):
    parent, activation, child = seeded_two_level(store, tmp_path)
    parent['runner_policy'] = 'guided'
    token = parent['pending'][0]
    token['decision_id'] = 'inspect-child'
    node = next(n for n in parent['definition']['nodes'] if n['id'] == 'call')
    request = {'workflow_run_id': child['workflow_run_id'], 'execution_id': child['activations'][0]['id'], 'view': 'result'}
    decision = {'decision_id': token['decision_id'], 'action': 'inspect', 'reason': 'Read child evidence', 'requests': [request]}
    normalized, warnings = d.normalize_decision(parent, node, token, decision, False, root=store.root)
    assert normalized['requests'] == [request]
    assert not warnings
    page = workflow_inspection.inspect_request(parent, store.root, normalized['requests'][0])
    assert page['workflow_run_id'] == child['workflow_run_id']
    unrelated = seed_parent(store, tmp_path, 'parent', stage='preparing')
    decision['requests'][0]['workflow_run_id'] = unrelated['workflow_run_id']
    normalized, _ = d.normalize_decision(parent, node, token, decision, False, root=store.root)
    with pytest.raises(ValueError):
        workflow_inspection.inspect_request(parent, store.root, normalized['requests'][0])
