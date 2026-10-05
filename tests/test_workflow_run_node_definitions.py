"""Run workflow node definitions: identity, references, pinning and authority."""
import asyncio
import copy
import json
import threading
from pathlib import Path

import pytest

from polybridge import workflows as w
from polybridge import workflow_references as refs


def child_definition(name: str = "child") -> dict:
    return {"name": name, "routing_mode": "explicit", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": "work", "type": "agent", "role": "task", "instructions": "Do the work", "agent": {"backend": "codex"}}, {"id": "end", "type": "end"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]}


def parent_definition(workflow_id: str, *, name: str = "parent", node_id: str = "call", mode: str = "child", optional: bool = False) -> dict:
    return {"name": name, "routing_mode": "explicit", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": node_id, "type": "workflow", "workflow_ref": {"workflow_id": workflow_id}, "orchestrator_mode": mode, "optional": optional, "instructions": "Delegate the focused assignment"}, {"id": "end", "type": "end"}], "connections": [{"id": "begin", "source": "start", "target": node_id}, {"id": "finish", "source": node_id, "target": "end"}]}


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    return w.WorkflowStore(tmp_path)


def test_workflow_ids_are_assigned_preserved_and_caller_supplied_ids_ignored(storage):
    first = storage.save("alpha", child_definition("alpha"))
    assert first["workflow_id"] and first["workflow_id"] != "legacy-"
    again = storage.save("alpha", child_definition("alpha"), first["revision"])
    assert again["workflow_id"] == first["workflow_id"]
    supplied = child_definition("alpha")
    supplied["workflow_id"] = "caller-chosen"
    ignored = storage.save("alpha", supplied, again["revision"])
    assert ignored["workflow_id"] == first["workflow_id"]


def test_legacy_definitions_report_name_derived_id_and_persist_on_next_save(storage):
    raw = w.validate_definition(child_definition("legacy-one"))
    raw["execution_contract"] = "delegation"
    raw["revision"] = 1
    w._write(storage.definitions / "legacy-one.json", raw)
    legacy = storage.get("legacy-one")
    assert legacy["workflow_id"] == "legacy-" + refs.workflow_identity(raw).removeprefix("legacy-")
    saved = storage.save("legacy-one", child_definition("legacy-one"), 1)
    assert saved["workflow_id"] == legacy["workflow_id"]


def test_missing_reference_refused_on_save_and_start(storage):
    saved = storage.save("child", child_definition())
    missing = parent_definition(saved["workflow_id"] + "0")
    with pytest.raises(w.WorkflowError, match="Unknown workflow reference"):
        storage.save("parent", missing)
    assert not (storage.definitions / "parent.json").exists()


async def test_missing_reference_refused_on_start(storage):
    pinned_child = storage.save("child", child_definition())
    parent = storage.save("parent", parent_definition(pinned_child["workflow_id"]))
    storage.delete("child")
    with pytest.raises(refs.DependencyError, match="Unknown workflow reference"):
        refs.resolve_dependencies(storage, definition=parent)


def test_direct_and_indirect_cycles_are_refused_with_the_path(storage):
    a = storage.save("a", child_definition("a"))
    b = storage.save("b", child_definition("b"))
    self_ref = parent_definition(a["workflow_id"], name="a")
    with pytest.raises(w.WorkflowError, match="a > a"):
        storage.save("a", self_ref, a["revision"])
    a_calls_b = storage.save("a", parent_definition(b["workflow_id"], name="a"), a["revision"])
    b_calls_a = parent_definition(a_calls_b["workflow_id"], name="b")
    with pytest.raises(w.WorkflowError, match="b > a > b"):
        storage.save("b", b_calls_a, b["revision"])
    assert storage.get("b")["revision"] == b["revision"]


def test_two_branches_may_call_one_workflow_and_retry_arrows_stay_valid(storage):
    shared = storage.save("shared", child_definition("shared"))
    both = {"name": "both", "routing_mode": "explicit", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": "left", "type": "workflow", "workflow_ref": {"workflow_id": shared["workflow_id"]}, "instructions": "Left"}, {"id": "right", "type": "workflow", "workflow_ref": {"workflow_id": shared["workflow_id"]}, "instructions": "Right"}, {"id": "join", "type": "parallel_end", "parallel_group_id": "g"}, {"id": "merge", "type": "agent", "instructions": "Reconcile", "agent": {"backend": "codex"}}, {"id": "end", "type": "end"}, {"id": "split", "type": "parallel_start", "parallel_group_id": "g"}], "connections": [{"id": "begin", "source": "start", "target": "split"}, {"id": "l", "source": "split", "target": "left"}, {"id": "r", "source": "split", "target": "right"}, {"id": "lj", "source": "left", "target": "join"}, {"id": "rj", "source": "right", "target": "join"}, {"id": "done", "source": "join", "target": "merge"}, {"id": "finish", "source": "merge", "target": "end"}]}
    saved = storage.save("both", both)
    tree = refs.resolve_dependencies(storage, definition=saved)
    assert sorted(edge["to"] for edge in tree["edges"]) == [shared["workflow_id"], shared["workflow_id"]]
    with_retry = copy.deepcopy(saved)
    with_retry["connections"].append({"id": "redo", "source": "merge", "target": "merge", "max_retries": 1})
    validated = w.validate_definition(with_retry)
    assert any(edge.get("backward") for edge in validated["connections"])


def test_depth_four_is_accepted_and_depth_five_refused(storage):
    first = storage.save("one", child_definition("one"))
    previous = first
    for index in range(2, 5):
        previous = storage.save(f"nest{index}", parent_definition(previous["workflow_id"], name=f"nest{index}"))
    deep = parent_definition(previous["workflow_id"], name="nest5")
    with pytest.raises(w.WorkflowError, match="nesting exceeds"):
        storage.save("nest5", deep)
    # The root counts as level 1: a chain of four definitions resolves.
    tree = refs.resolve_dependencies(storage, definition=storage.get("nest4"))
    assert max(entry["depth"] for entry in tree["access"].values()) == 4


def test_cyclic_save_through_builder_guard_writes_nothing(storage):
    saved = storage.save("child", child_definition())
    before = storage.get("child")
    cyclic = parent_definition(saved["workflow_id"], name="child")
    def guard(tree):
        raise ValueError("authority refused")
    # The builder passes its guard as authority_guard; the cycle is refused before
    # the guard runs and before anything is written.
    with pytest.raises(w.WorkflowError, match="cycle"):
        storage.save("child", cyclic, before["revision"], authority_guard=guard)
    assert storage.get("child") == before


def test_authority_guard_refusal_leaves_saved_definition_unchanged(storage):
    saved = storage.save("child", child_definition())
    candidate = child_definition("child")
    candidate["nodes"][1]["freedom"] = "unrestricted"
    def guard(tree):
        raise ValueError("out of envelope")
    with pytest.raises(ValueError, match="out of envelope"):
        storage.save("child", candidate, saved["revision"], authority_guard=guard)
    assert storage.get("child")["nodes"][1].get("freedom", "publish") != "unrestricted"


def test_concurrent_cross_saves_fail_exactly_one(storage):
    a = storage.save("a", child_definition("a"))
    b = storage.save("b", child_definition("b"))
    outcomes: list[bool] = []

    def save_a():
        try:
            storage.save("a", parent_definition(b["workflow_id"], name="a"), a["revision"])
            outcomes.append(True)
        except w.WorkflowError:
            outcomes.append(False)

    def save_b():
        try:
            storage.save("b", parent_definition(a["workflow_id"], name="b"), b["revision"])
            outcomes.append(True)
        except w.WorkflowError:
            outcomes.append(False)

    threads = [threading.Thread(target=save_a), threading.Thread(target=save_b)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(outcomes) == [False, True]


def test_save_does_not_deadlock(storage):
    first = storage.save("one", child_definition("one"))
    second = storage.save("two", child_definition("two"))
    errors: list[Exception] = []

    def save(name: str, definition: dict, revision: int):
        try:
            storage.save(name, definition, revision)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=save, args=("one", child_definition("one"), first["revision"])), threading.Thread(target=save, args=("two", parent_definition(first["workflow_id"], name="two"), second["revision"]))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert not any(thread.is_alive() for thread in threads)
    assert not errors


def test_edits_and_deletes_after_start_do_not_change_the_pin(storage):
    child = storage.save("child", child_definition())
    parent = storage.save("parent", parent_definition(child["workflow_id"]))
    run = storage.create_run(w.validate_definition(parent), "go", storage.root, permission_policy="saved_node", dependency_tree=refs.resolve_dependencies(storage, definition=parent))
    pinned = run["dependency_tree"]["workflows"][child["workflow_id"]]
    changed = child_definition("child")
    changed["nodes"][1]["instructions"] = "Different instructions"
    storage.save("child", changed, child["revision"])
    storage.delete("child")
    assert run["dependency_tree"]["workflows"][child["workflow_id"]] == pinned
    assert run["dependency_tree"]["workflows"][child["workflow_id"]]["definition"]["nodes"][1]["instructions"] == "Do the work"


def test_save_rewrites_workflow_name_caches_from_the_pinned_tree(storage):
    child = storage.save("child", child_definition("child"))
    parent = parent_definition(child["workflow_id"])
    parent["nodes"][1]["workflow_name"] = "stale cache"
    saved = storage.save("parent", parent)
    assert saved["nodes"][1]["workflow_name"] == "child"


def test_access_summary_and_edges_carry_mode(storage):
    child = storage.save("child", child_definition())
    parent = storage.save("parent", parent_definition(child["workflow_id"], mode="current"))
    tree = refs.resolve_dependencies(storage, definition=parent)
    edge = tree["edges"][0]
    assert edge["orchestrator_mode"] == "current"
    summary = tree["access"][child["workflow_id"]]
    assert summary["max_freedom"] == "publish"
    assert summary["depth"] == 2
    assert "codex" in summary["backends"]


def test_delete_reports_referenced_by_notice(storage):
    child = storage.save("child", child_definition())
    storage.save("parent", parent_definition(child["workflow_id"]))
    result = storage.delete("child")
    assert result["referenced_by"] == ["parent"]
    assert "parent" in result["notice"]


def test_workflow_validate_reports_dependencies(tmp_path, monkeypatch, capsys):
    from polybridge import ctl
    monkeypatch.setenv("HOME", str(tmp_path))
    root = tmp_path / ".polybridge"
    child_store = w.WorkflowStore(root)
    child = child_store.save("child", child_definition())
    definition_file = tmp_path / "parent.json"
    definition_file.write_text(json.dumps(parent_definition(child["workflow_id"])))
    code = ctl.main(["workflow-validate", "--definition", str(definition_file), "--json"])
    assert code == 0
    document = json.loads(capsys.readouterr().out)["result"]
    assert document["valid"] is True
    assert document["dependencies"]["root_workflow_id"] == refs.workflow_identity(w.validate_definition(parent_definition(child["workflow_id"])))
    assert document["dependencies"]["edges"][0]["to"] == child["workflow_id"]
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(parent_definition("missing-id")))
    code = ctl.main(["workflow-validate", "--definition", str(broken), "--json"])
    assert code == 0
    document = json.loads(capsys.readouterr().out)["result"]
    assert document["valid"] is False
    assert "Unknown workflow reference" in document["error"]


def test_shared_dependency_is_checked_on_every_depth_path(storage):
    leaf = storage.save("leaf", child_definition("leaf"))
    middle = storage.save("middle", parent_definition(leaf["workflow_id"], name="middle"))
    deeper = storage.save("deeper", parent_definition(middle["workflow_id"], name="deeper"))
    root = parent_definition(leaf["workflow_id"], name="root")
    root["nodes"].insert(2, {"id": "deep", "type": "workflow", "workflow_ref": {"workflow_id": deeper["workflow_id"]}})
    root["connections"][1]["target"] = "deep"
    root["connections"].append({"id": "last", "source": "deep", "target": "end"})
    saved = storage.save("root", root)
    tree = refs.resolve_dependencies(storage, definition=saved)
    assert tree["access"][leaf["workflow_id"]]["depth"] == 4
    assert len(tree["edges"]) == 4
    with pytest.raises(w.WorkflowError, match="nesting exceeds"):
        storage.save("too-deep", parent_definition(saved["workflow_id"], name="too-deep"))


async def test_launch_override_is_in_the_pinned_root(storage, tmp_path):
    definition = storage.save("override-root", child_definition("override-root"))
    launched = w.launch_definition(definition, {"model": "override-model"})
    tree = refs.resolve_dependencies(storage, definition=launched, substitute_name="override-root")
    run = await w.start_workflow("override-root", "assignment", tmp_path, root=storage.root, overrides={"model": "override-model"}, definition_snapshot=launched, dependency_tree=tree)
    assert run["definition"]["orchestrator"]["model"] == "override-model"
    assert run["dependency_tree"]["workflows"][tree["root_workflow_id"]]["definition"]["orchestrator"]["model"] == "override-model"
    assert "model" not in storage.get("override-root")["orchestrator"]


async def test_launch_refuses_a_tree_pinned_before_orchestrator_override(storage, tmp_path):
    definition = storage.save("override-root", child_definition("override-root"))
    stale_tree = refs.resolve_dependencies(storage, definition=definition, substitute_name="override-root")
    with pytest.raises(w.WorkflowError, match="does not match the starting definition"):
        await w.start_workflow("override-root", "assignment", tmp_path, root=storage.root, overrides={"model": "override-model"}, definition_snapshot=definition, dependency_tree=stale_tree)
