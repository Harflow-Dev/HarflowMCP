"""What a pending launch reports and who it is cancelled with, and where the ledger is read."""

from __future__ import annotations

import threading

import pytest

from harflow_mcp import lineage, resources, store, task_resources
from harflow_mcp.backends import get

from restart_test_support import install_fake_claude
from test_task_resources import OWNER, ledger, owned, registry, tasks_dir, wait_for, write_receipt  # noqa: F401
from test_worktrees import record_for  # noqa: F401


# ---- item 11: enforcement is the caller's plan or nothing ----------------------------------------


@pytest.mark.asyncio
async def test_a_pending_launch_reports_the_planned_enforcement_not_a_nominal_one(tmp_path, monkeypatch, identities) -> None:
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("slow 6 hold", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    waiter = await reg.start("reply two", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    receipt = task_resources.pending_receipt(log_dir, waiter.task_id)
    planned = receipt["enforcement"]
    assert planned == get("claude").enforcement("read_only", None).as_dict()  # the caller's plan, recorded with the receipt
    view = task_resources.pending_view(log_dir, waiter.task_id)
    assert view["enforcement"] == planned and waiter.enforcement == planned
    assert "nominal" not in view.get("enforcement_note", "").lower()


def test_a_pending_launch_without_a_plan_reports_null_enforcement(tmp_path, identities, owned) -> None:
    write_receipt(tmp_path, "a" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"})
    view = task_resources.pending_view(owned.tasks_directory, "a" * 32)
    assert view["enforcement"] is None and "enforcement_note" not in view
    assert task_resources.PendingLaunch(owned.tasks_directory, task_resources.pending_receipt(owned.tasks_directory, "a" * 32)).enforcement is None


# ---- item 12: the cascade reaches pending grandchildren under recorded intermediates -------------


def test_pending_descendants_are_found_under_recorded_intermediates(tmp_path, identities, owned) -> None:
    log_dir = owned.tasks_directory
    write_receipt(tmp_path, "c" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"},
                  spawned_by="b" * 32, root_task_id="a" * 32)
    write_receipt(tmp_path, "d" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"},
                  spawned_by="x" * 32)
    # B is a recorded task, so it is not itself pending; without it the walk never reaches C.
    assert task_resources.pending_descendants(log_dir, "a" * 32) == ["c" * 32]  # via root_task_id
    write_receipt(tmp_path, "c" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"}, spawned_by="b" * 32)
    assert task_resources.pending_descendants(log_dir, "a" * 32) == []
    assert task_resources.pending_descendants(log_dir, "a" * 32, seeds=["b" * 32]) == ["c" * 32]


@pytest.mark.asyncio
async def test_cascade_cancels_a_pending_grandchild_under_a_recorded_intermediate(tmp_path, monkeypatch, identities) -> None:
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    root = await reg.start("slow 30 root", tmp_path, backend=get("claude"), freedom="read_only", task_id="a" * 32)
    await wait_for(lambda: store.read(log_dir, root.task_id) is not None)
    store.write(log_dir, record_for("b" * 32, str(tmp_path), spawned_by=root.task_id, root_task_id=root.task_id))
    write_receipt(tmp_path, "c" * 32, "waiting_for_resources", owner=OWNER, resources={"names": ["sim"], "state": "waiting"},
                  spawned_by="b" * 32, repo_path=str(tmp_path))
    identities.alive(OWNER["pid"])
    result = await reg.cancel_cascade(root.task_id)
    assert result["pending_cancel_requested"] == ["c" * 32]
    assert task_resources.cancel_path(log_dir, "c" * 32).exists()


# ---- item 15: ledger reads on the request path stay off the event loop ---------------------------


@pytest.mark.asyncio
async def test_a_nested_request_checks_the_ledger_off_the_event_loop(tmp_path, monkeypatch, identities) -> None:
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    parent = await reg.start("slow 6 one", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="a" * 32)
    await wait_for(lambda: store.read(log_dir, parent.task_id) is not None and store.read(log_dir, parent.task_id).resources)
    caller = lineage.Caller(store.read(log_dir, parent.task_id), "session")
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(caller))
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: caller)
    main = threading.main_thread()
    threads: list[bool] = []
    real = resources.ResourceLedger._read_only

    def spy(self):
        threads.append(threading.current_thread() is main)
        return real(self)

    monkeypatch.setattr(resources.ResourceLedger, "_read_only", spy)
    await reg.start("reply child", tmp_path, backend=get("claude"), freedom="read_only", resources=["sim"], task_id="b" * 32)
    assert threads and not any(threads)
