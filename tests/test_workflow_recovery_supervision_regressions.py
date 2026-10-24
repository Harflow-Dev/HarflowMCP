"""Deterministic workflow scheduler recovery and native replay barriers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import workflow_native_observations as native_recovery
from harflow_mcp import workflow_recovery


class _Storage:
    root = Path("/state")

    def __init__(self, runs):
        self.runs = runs
        self.updates = []

    def get_run(self, run_id, **kwargs):
        return self.runs[run_id]

    def update_run(self, run_id, mutate, reason):
        mutate(self.runs[run_id])
        self.updates.append((run_id, reason))
        return self.runs[run_id]


def _root(*, owner=None):
    return {
        "workflow_run_id": "root",
        "execution_contract": "delegation",
        "status": "running",
        "sequence": 4,
        "supervisor_identity": owner,
        "activations": [{"id": "a", "role": "node", "status": "running", "tasks": []}],
    }


def test_recovery_inventory_follows_only_reciprocal_child_links():
    root = _root()
    root["activations"][0]["invocation"] = {"child_workflow_run_id": "child"}
    child = {"workflow_run_id": "child", "parent_link": {"workflow_run_id": "root", "execution_id": "a", "root_workflow_run_id": "root"}, "activations": []}
    storage = _Storage({"root": root, "child": child})
    assert [run["workflow_run_id"] for run in workflow_recovery.inventory(storage, root)] == ["root", "child"]

    child["parent_link"]["execution_id"] = "wrong"
    with pytest.raises(Exception, match="ownership"):
        workflow_recovery.inventory(storage, root)


def test_recovery_request_rejects_live_or_uncertain_scheduler(monkeypatch):
    root = _root(owner={"pid": 7, "start_time": "t", "markers": []})
    storage = _Storage({"root": root})
    monkeypatch.setattr("harflow_mcp.identity.identity_check", lambda _owner: "alive")
    with pytest.raises(Exception, match="live or uncertain"):
        workflow_recovery.request(storage, root, "operator recovery", "caller")


def test_restore_permits_holds_active_task_and_control_slots(monkeypatch):
    root = _root(owner={"pid": 7, "start_time": "t", "markers": []})
    root["activations"][0]["tasks"] = [{"task_id": "t1", "status": "running", "dispatch_stage": "spawn_confirmed"}]

    class Tree:
        from harflow_mcp.workflow_invocation import TreeSlots

        slots = TreeSlots(3)
        control_slots = TreeSlots(2)

        def __init__(self):
            self.held = []

        def hold_slot(self, run_id, task_id, *, control=False):
            self.held.append((run_id, task_id, control))

    tree = Tree()
    supervisor = SimpleNamespace(store=_Storage({"root": root}), run=lambda **_: root, tree=tree)
    workflow_recovery.restore_permits(supervisor)
    assert tree.slots.free == 2
    assert tree.held == [("root", "t1", False)]


def test_native_replay_requires_final_watermark_before_terminal_update(monkeypatch):
    updates = []
    supervisor = SimpleNamespace(
        registry=SimpleNamespace(_log_dir=Path("/state/tasks")),
        _task_update=lambda *args: updates.append(args),
    )
    replay = native_recovery.Replay(supervisor, "transport", lambda _event: None)
    monkeypatch.setattr("harflow_mcp.task_observations.read", lambda *_args, **_kwargs: ([{"sequence": 1, "generation": "g", "event": {}}], None))
    with pytest.raises(ValueError, match="final observation barrier"):
        replay.poll(terminal=True)
