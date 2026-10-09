"""Permission aggregation follows pinned owner boundaries and launch disclosure."""
import copy

import pytest

from polybridge import workflows as w
from polybridge.workflow_native_policy import plan_owner_contracts, orchestrator_contract
from polybridge.workflow_references import workflow_identity
from test_workflow_delegation import graph


class CertifiedAdapter:
    def eligible(self, parent, candidate, settings):
        if w.FREEDOMS.index(settings["freedom"]) > w.FREEDOMS.index(parent.freedom):
            return "Insufficient owner access"
        if settings["network"] != parent.network:
            return "Cannot narrow network"
        return None


@pytest.fixture
def certified(monkeypatch):
    monkeypatch.setattr("polybridge.backends.native.adapter", lambda backend: CertifiedAdapter())
    monkeypatch.setattr(w.backends, "version", lambda _: "certified")


def definition(name, access="write_in_repo"):
    result = graph("implementation" if access != "read_only" else "review")
    result["name"] = name
    result["workflow_id"] = name
    result["nodes"][1].update(freedom=access, execution_mode="prefer_subagent", session_mode="agent_decides")
    return w.validate_definition(result)


def test_saved_access_and_legacy_ceiling(certified):
    d = definition("root", "publish")
    saved = plan_owner_contracts(d, freedom="read_only", permission_policy="saved_node")
    assert orchestrator_contract({"definition": d, "owner_contracts": saved})["freedom"] == "publish"
    d["nodes"][1]["role"] = "task"
    legacy = plan_owner_contracts(d, freedom="read_only", permission_policy="legacy_ceiling")
    assert orchestrator_contract({"definition": d, "owner_contracts": legacy})["freedom"] == "read_only"


def test_current_descendants_share_owner_child_descendants_do_not(certified):
    root = definition("root", "read_only")
    current = definition("current", "write_in_repo")
    child = definition("child", "publish")
    current["nodes"].append({"id": "child-call", "type": "workflow", "workflow_ref": {"workflow_id": "child"}, "orchestrator_mode": "child"})
    root["nodes"].append({"id": "current-call", "type": "workflow", "workflow_ref": {"workflow_id": "current"}, "orchestrator_mode": "current"})
    tree = {"workflows": {d["workflow_id"]: {"definition": d} for d in (root, current, child)}}
    plan = plan_owner_contracts(root, tree)
    assert set(plan["owners"]) == {"root", "child"}
    owner = orchestrator_contract({"definition": root, "owner_contracts": plan})
    assert owner["freedom"] == "write_in_repo"
    assert {n["workflow_id"] for n in owner["contributing_nodes"]} == {"root", "current"}
    assert next(iter(plan["owners"]["child"]["candidates"].values()))["freedom"] == "publish"


def test_incompatible_network_does_not_leave_unneeded_write_access(certified):
    d = definition("root")
    d["nodes"][1]["network"] = False
    review = copy.deepcopy(d["nodes"][1])
    review.update(id="review", role="review", freedom="read_only", network=True)
    d["nodes"].append(review)
    plan = plan_owner_contracts(d, network=None)
    owner = orchestrator_contract({"definition": d, "owner_contracts": plan})
    assert owner["freedom"] == "read_only"
    assert owner["network"] is True
    assert [n["node_id"] for n in owner["contributing_nodes"]] == ["review"]


def test_historical_owner_is_not_recomputed(certified):
    d = definition("root", "publish")
    assert orchestrator_contract({"definition": d, "network": False}) == {"freedom": "read_only", "network": False}


def test_candidate_changed_after_pin_fails_closed(certified):
    d = definition("root")
    plan = plan_owner_contracts(d)
    with pytest.raises(ValueError, match="no pinned"):
        orchestrator_contract({"definition": d, "owner_contracts": plan}, {"backend": "codex", "model": "changed"})


def test_matching_node_fallback_contributes_without_skipping_primary(certified):
    d = definition("root")
    d["nodes"][1]["agent"] = {"backend": "opencode", "fallbacks": [{"backend": "codex"}]}
    plan = plan_owner_contracts(d)
    owner = orchestrator_contract({"definition": d, "owner_contracts": plan})
    assert owner["freedom"] == "write_in_repo"
    assert [node["execution_kind"] for node in owner["nodes"]] == ["headless", "native_subagent"]
    assert owner["contributing_nodes"][0]["candidate"] == {"backend": "codex"}
    assert owner["contributing_nodes"][0]["candidate_position"] == 1


def test_matching_owner_fallback_has_its_own_permission_contract(certified):
    d = definition("root")
    d["orchestrator"] = {"backend": "claude", "fallbacks": [{"backend": "codex"}]}
    plan = plan_owner_contracts(d)
    run = {"definition": d, "owner_contracts": plan}
    assert orchestrator_contract(run, d["orchestrator"])["freedom"] == "read_only"
    assert orchestrator_contract(run, {"backend": "codex"})["freedom"] == "write_in_repo"


def test_current_descendant_matching_fallback_is_aggregated(certified):
    root = definition("root", "read_only")
    current = definition("current", "publish")
    current["nodes"][1]["agent"] = {"backend": "opencode", "fallbacks": [{"backend": "codex"}]}
    root["nodes"].append({"id": "call", "type": "workflow", "workflow_ref": {"workflow_id": "current"}, "orchestrator_mode": "current"})
    tree = {"workflows": {"current": {"definition": current}}}
    owner = orchestrator_contract({"definition": root, "owner_contracts": plan_owner_contracts(root, tree)})
    assert owner["freedom"] == "publish"
    contribution = next(node for node in owner["contributing_nodes"] if node["workflow_id"] == "current")
    assert contribution["candidate_position"] == 1


def test_current_owner_recurses_without_adopting_nested_orchestrator(certified):
    root = definition("root", "read_only")
    middle = definition("middle", "read_only")
    deep = definition("deep", "write_in_repo")
    middle["orchestrator"] = {"backend": "opencode"}
    deep["orchestrator"] = {"backend": "opencode"}
    for parent, target in ((root, middle), (middle, deep)):
        parent["nodes"].append({"id": "call", "type": "workflow", "workflow_ref": {"workflow_id": target["workflow_id"]}, "orchestrator_mode": "current"})
    tree = {"workflows": {d["workflow_id"]: {"definition": d} for d in (middle, deep)}}
    plan = plan_owner_contracts(root, tree)
    assert set(plan["owners"]) == {"root"}
    owner = orchestrator_contract({"definition": root, "owner_contracts": plan})
    assert owner["freedom"] == "write_in_repo"
    assert {n["workflow_id"] for n in owner["contributing_nodes"]} == {"root", "middle", "deep"}


def test_preview_hash_rejects_changes_before_launch(tmp_path, monkeypatch, certified):
    storage = w.WorkflowStore(tmp_path / "state")
    d = storage.save("root", definition("root"))
    repo = tmp_path / "repo"
    repo.mkdir()
    preview = w.preview_workflow_run("root", repo, root=storage.root)
    launched = []
    monkeypatch.setattr(w, "_launch", lambda *args: launched.append(args))
    import asyncio
    with pytest.raises(w.WorkflowError, match="preview changed"):
        asyncio.run(w.start_workflow("root", "Go", repo, root=storage.root, network=False, expected_preview_hash=preview["preview_hash"], _verified_caller=None))
    assert launched == []
    assert storage.list_runs() == []
    run = asyncio.run(w.start_workflow("root", "Go", repo, root=storage.root, expected_preview_hash=preview["preview_hash"], _verified_caller=None))
    assert run["owner_contracts"] == preview["owner_contracts"]
    assert run["definition"]["context_delivery"] == "optimized_v1"


def test_repeated_and_diamond_current_definitions_are_collected_once_per_owner(certified):
    root, left, right, leaf, child = [definition(name) for name in ("root", "left", "right", "leaf", "child")]
    def link(parent, target, ident, mode="current"):
        parent["nodes"].append({"id": ident, "type": "workflow", "workflow_ref": {"workflow_id": target["workflow_id"]}, "orchestrator_mode": mode})
    link(root, left, "left")
    link(root, left, "left-again")
    link(root, right, "right")
    link(left, leaf, "leaf")
    link(right, leaf, "leaf")
    link(root, child, "child", "child")
    link(child, leaf, "leaf")
    tree = {"workflows": {d["workflow_id"]: {"definition": d} for d in (left, right, leaf, child)}}
    plan = plan_owner_contracts(root, tree)
    root_owner = orchestrator_contract({"definition": root, "owner_contracts": plan})
    assert len(root_owner["nodes"]) == len(root_owner["contributing_nodes"]) == 4
    assert {n["workflow_id"] for n in root_owner["nodes"]} == {"root", "left", "right", "leaf"}
    child_owner = next(iter(plan["owners"]["child"]["candidates"].values()))
    assert {n["workflow_id"] for n in child_owner["nodes"]} == {"child", "leaf"}


def test_current_collection_still_rejects_cycles(certified):
    root, child = definition("root"), definition("child")
    for parent, target in ((root, child), (child, root)):
        parent["nodes"].append({"id": "call", "type": "workflow", "workflow_ref": {"workflow_id": target["workflow_id"]}, "orchestrator_mode": "current"})
    with pytest.raises(ValueError, match="Cyclic"):
        plan_owner_contracts(root, {"workflows": {"child": {"definition": child}}})


def test_planning_caches_backend_versions_per_operation(certified, monkeypatch):
    calls = []
    def version(backend):
        calls.append(backend.name)
        return "certified"
    monkeypatch.setattr(w.backends, "version", version)
    root = definition("root")
    for index in range(50):
        node = copy.deepcopy(root["nodes"][1])
        node["id"] = f"node-{index}"
        root["nodes"].append(node)
    plan_owner_contracts(root)
    assert len(calls) == 1
    plan_owner_contracts(root)
    assert len(calls) == 2  # A new preview/start gets fresh evidence.
