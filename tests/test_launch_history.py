"""Regression coverage for historical task launch receipt identity drift."""
from __future__ import annotations

from pathlib import Path

import pytest

from harflow_mcp import followups, inbox, server, store, task_resources, task_supervision
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from harflow_mcp.tasks import TaskRegistry


@pytest.fixture
def owned_log_dir(tmp_path):
    layout = resolve_layout(root=tmp_path / "state")
    layout.root.mkdir()
    with state_session(layout):
        layout.tasks_directory.mkdir()
        yield layout.tasks_directory


def _receipt(log_dir: Path, task_id: str, receipt_state: str, **overrides) -> dict:
    info = log_dir.stat()
    receipt = {
        "format": task_supervision.FORMAT,
        "product": "harflow",
        "product_version": task_supervision.PRODUCT_VERSION,
        "v": 1,
        "task_id": task_id,
        "generation": "c" * 32,
        "root": str(log_dir.parent),
        # Same inode, stale device value: the observed historical drift.
        "directory_identity": [info.st_dev + 1, info.st_ino],
        "state": receipt_state,
        "request_sha256": "d" * 64,
        "configuration_sha256": "e" * 64,
        "backend": "claude",
        "session_id": f"session-{task_id[:4]}",
        "parent_task_id": None,
        "chain_root_task_id": None,
        "scratch_chain_id": None,
        "owner": None,
        "repo_path": str(log_dir.parent),
        "freedom": "read_only",
    }
    receipt.update(overrides)
    return receipt


def _write_receipt(log_dir: Path, task_id: str, receipt_state: str, **overrides) -> dict:
    receipt = _receipt(log_dir, task_id, receipt_state, **overrides)
    task_supervision._write(log_dir / f"{task_id}.launch.json", receipt)
    return receipt


def _record(task_id: str, *, session_id="target-session", status="completed", **overrides):
    return store.TaskRecord(
        task_id=task_id,
        backend="claude",
        session_id=session_id,
        repo_path="/tmp/project",
        started_at="2026-01-01T00:00:00+00:00",
        status=status,
        **overrides,
    )


@pytest.mark.parametrize("state", ["settled", "not_started"])
def test_unrelated_terminal_device_drift_does_not_block_session_liveness(owned_log_dir, state):
    store.write(owned_log_dir, _record("a" * 32))
    receipt_path = owned_log_dir / ("b" * 32 + ".launch.json")
    _write_receipt(owned_log_dir, "b" * 32, state)
    original_receipt = receipt_path.read_bytes()

    assert store.live_session_ids(owned_log_dir) == set()
    assert TaskRegistry(log_dir=owned_log_dir).session_has_live_run("target-session") is False
    assert receipt_path.read_bytes() == original_receipt


@pytest.mark.parametrize("state", ["reserved", "claimed", "waiting_for_resources", "granted", "spawned"])
def test_each_unresolved_state_accepts_legacy_inode_after_device_drift(owned_log_dir, state):
    task_id = "c" * 32
    _write_receipt(owned_log_dir, task_id, state)

    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == state
    assert task_supervision.reservations(owned_log_dir)[0]["task_id"] == task_id


def test_direct_task_launch_read_accepts_legacy_terminal_device_drift(owned_log_dir):
    task_id = "d" * 32
    _write_receipt(owned_log_dir, task_id, "settled")

    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == "settled"


def test_exact_identity_terminal_receipt_remains_accepted(owned_log_dir):
    task_id = "p" * 32
    receipt = _receipt(owned_log_dir, task_id, "settled")
    receipt["directory_identity"] = [owned_log_dir.stat().st_dev, owned_log_dir.stat().st_ino]
    task_supervision._write(owned_log_dir / f"{task_id}.launch.json", receipt)

    assert task_supervision.read_launch(owned_log_dir, task_id)["state"] == "settled"
    assert task_supervision.reservations(owned_log_dir)[0]["task_id"] == task_id


def test_launch_scan_isolates_root_and_inode_mismatches(owned_log_dir):
    info = owned_log_dir.stat()
    invalid = [
        {"root": str(owned_log_dir.parent / "elsewhere")},
        {"directory_identity": [info.st_dev + 1, info.st_ino + 1]},
    ]
    for index, changes in enumerate(invalid):
        task_id = f"e{index}".ljust(32, "e")
        _write_receipt(owned_log_dir, task_id, "settled", **changes)
        scan = task_supervision.scan_launches(owned_log_dir)
        assert scan.entries == []
        assert task_id in scan.refusals
        assert scan.refusals[task_id].code in {"root_mismatch", "identity_mismatch"}
        (owned_log_dir / f"{task_id}.launch.json").unlink()


@pytest.mark.parametrize("identity", [lambda dev, ino: [True, ino], lambda dev, ino: [dev + 1.0, ino], lambda dev, ino: [dev + 1]])
def test_launch_scan_reports_malformed_legacy_directory_identity(owned_log_dir, identity):
    info = owned_log_dir.stat()
    task_id = "f" * 32
    _write_receipt(owned_log_dir, task_id, "not_started", directory_identity=identity(info.st_dev, info.st_ino))

    scan = task_supervision.scan_launches(owned_log_dir)
    assert scan.entries == []
    assert scan.refusals[task_id].code == "field_shape"


@pytest.mark.parametrize("changes", [
    {"generation": "not-a-generation"},
    {"request_sha256": "bad"},
    {"backend": 7},
    {"followups": {"task_id": "bad", "ids": "not-a-list"}},
])
def test_launch_scan_reports_other_schema_errors(owned_log_dir, changes):
    task_id = "g" * 32
    _write_receipt(owned_log_dir, task_id, "settled", **changes)

    scan = task_supervision.scan_launches(owned_log_dir)
    assert scan.entries == []
    assert scan.refusals[task_id].code in {"generation", "field_shape"}


def test_terminal_successor_remains_visible_and_seals_queued_followup(owned_log_dir):
    source_id, successor_id = "h" * 32, "i" * 32
    source = _record(source_id)
    store.write(owned_log_dir, source)
    queued = followups.send(owned_log_dir, source_id, "queued before successor", by=None)
    _write_receipt(
        owned_log_dir, successor_id, "settled", parent_task_id=source_id,
        session_id=source.session_id,
    )

    assert successor_id in store.session_successor_task_ids(owned_log_dir, source_id, source.session_id)
    assert successor_id in followups.successor_candidates(owned_log_dir, source)
    with pytest.raises(inbox.SendRefused) as refused:
        followups.send(owned_log_dir, source_id, "must not supersede the successor", by=None)
    assert refused.value.code == "superseded"
    assert queued["status"] == "queued"


def test_drifted_not_started_launch_with_unknown_followup_delivery_is_not_released(
    owned_log_dir, monkeypatch,
):
    task_id = "r" * 32
    receipt = _receipt(
        owned_log_dir, task_id, "not_started", followups_delivery_unknown=True,
        resources={"names": ["checkout:repo"], "reservation_id": "s" * 32, "state": "not_started"},
    )
    task_supervision._write(owned_log_dir / f"{task_id}.launch.json", receipt)
    monkeypatch.setattr(followups.identity, "identity_check", lambda _owner: "dead")
    item = {
        "status": "reserved", "successor_task_id": task_id,
        "owner": {"pid": 999999, "start_time": "gone", "markers": []},
    }

    rows = task_supervision.reservations(owned_log_dir)

    assert rows[0]["state"] == "not_started"
    assert rows[0]["followups_delivery_unknown"] is True
    assert followups._launch_verdict(owned_log_dir, task_id) == followups.DELIVERY_UNKNOWN
    assert followups._reserved_verdict(owned_log_dir, item) == followups.DELIVERY_UNKNOWN


def test_queued_message_path_ignores_unrelated_terminal_device_drift(owned_log_dir):
    source_id = "j" * 32
    store.write(owned_log_dir, _record(source_id))
    _write_receipt(owned_log_dir, "k" * 32, "settled")

    assert followups.successor_candidates(owned_log_dir, store.read(owned_log_dir, source_id)) == []
    queued = followups.send(owned_log_dir, source_id, "a queued follow-up", by=None)

    assert queued["status"] == "queued"


@pytest.mark.parametrize(("metadata", "stale_identity"), [
    ({"resources": {"names": ["checkout:repo"], "reservation_id": "resource-id", "state": "not_started"}}, True),
    ({"worktree": {"path": "/tmp/project/worktree", "state": "planned"}}, True),
    ({"resources": {"names": ["checkout:repo"], "reservation_id": "current-id", "state": "not_started"}}, False),
])
@pytest.mark.asyncio
async def test_list_tasks_renders_not_started_historical_resource_and_worktree_receipts(
    owned_log_dir, monkeypatch, metadata, stale_identity,
):
    task_id = "m" * 32
    receipt_path = owned_log_dir / f"{task_id}.launch.json"
    receipt = _receipt(owned_log_dir, task_id, "not_started", **metadata)
    if not stale_identity:
        info = owned_log_dir.stat()
        receipt["directory_identity"] = [info.st_dev, info.st_ino]
    task_supervision._write(receipt_path, receipt)
    original_receipt = receipt_path.read_bytes()
    registry = TaskRegistry(log_dir=owned_log_dir)
    monkeypatch.setattr(server, "_registry", registry)
    async def human_reader():
        return None
    monkeypatch.setattr(server, "_managed_workflow_reader", human_reader)

    views = task_resources.pending_views(owned_log_dir)
    assert [view["task_id"] for view in views] == [task_id]
    listing = await server.list_tasks()
    assert task_id in {entry["task_id"] for entry in listing}
    row = next(entry for entry in listing if entry["task_id"] == task_id)
    assert row["not_started"] is True
    assert row["launch_state"] == "not_started"
    assert receipt_path.read_bytes() == original_receipt


@pytest.mark.asyncio
async def test_public_list_tasks_continues_on_unresolved_device_drift(owned_log_dir, monkeypatch):
    task_id = "t" * 32
    _write_receipt(owned_log_dir, task_id, "waiting_for_resources",
                   resources={"names": ["checkout:repo"], "reservation_id": "u" * 32})
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=owned_log_dir))

    async def human_reader():
        return None

    monkeypatch.setattr(server, "_managed_workflow_reader", human_reader)
    result = await server.list_tasks()
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_per_id_pending_status_accepts_stale_terminal_device(owned_log_dir, monkeypatch):
    task_id = "n" * 32
    _write_receipt(owned_log_dir, task_id, "not_started", resources={"names": ["checkout:repo"]})
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=owned_log_dir))
    async def human_reader(_task_id):
        from harflow_mcp import id_resolution
        return id_resolution.Resolution(_task_id, None)
    monkeypatch.setattr(server, "_guard_task_read", human_reader)

    status = await server.get_task_status(task_id)
    assert status["task_id"] == task_id
