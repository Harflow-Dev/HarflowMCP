"""Observable state inspection and explicit backup/reset recovery boundaries."""
from __future__ import annotations

import json
import fcntl
import os
from pathlib import Path
import subprocess
import sys

import pytest

from harflow_mcp import state_recovery, state_compatibility, state_ownership
from harflow_mcp.state_ownership import StateOwnershipError
from harflow_mcp.state_layout import resolve_layout


def _marker(*, product: str = "harflow", version: str = "0.0.1") -> bytes:
    return (json.dumps({
        "product": product,
        "format": "harflow.store",
        "v": 1,
        "product_version": version,
        "layout_version": 1,
    }, sort_keys=True) + "\n").encode()


def _task_owner_document(*, status: str, pid="absent", owner="absent", exit_code="absent") -> bytes:
    document = {"product": "harflow", "format": "harflow.task", "product_version": "0.0.1", "v": 1,
                "status": status}
    if pid != "absent":
        document["pid"] = pid
    if owner != "absent":
        document["owner"] = owner
    if exit_code != "absent":
        document["exit_code"] = exit_code
    return (json.dumps(document, sort_keys=True) + "\n").encode()


def test_inspect_is_side_effect_free_and_returns_structured_reason(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    payload = b"foreign bytes\n"
    (root / "foreign.data").write_bytes(payload)
    before = {entry.name: entry.read_bytes() for entry in root.iterdir() if entry.is_file()}

    result = state_recovery.inspect_state(resolve_layout(root=root))

    assert result["status"] == "incompatible"
    assert result["reason"] == "unmarked_nonempty"
    assert result["state_root"] == str(root)
    assert result["recovery_supported"] is True
    assert result["cooperative_ownership_only"] is True
    assert result["recovery"]["operation_id"]
    assert result["recovery"]["confirmation_token"]
    assert {entry.name: entry.read_bytes() for entry in root.iterdir() if entry.is_file()} == before
    assert not (root / ".harflow-store.lock").exists()
    assert not (root / ".harflow-recovery.json").exists()


def test_inspect_ready_root_has_no_recovery_proposal(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / ".harflow-store.json").write_bytes(_marker())
    (root / ".harflow-store.lock").touch()

    result = state_recovery.inspect_state(resolve_layout(root=root))

    assert result["status"] == "ready"
    assert result["reason"] == "compatible"
    assert result["recovery_supported"] is False
    assert "recovery" not in result


@pytest.mark.parametrize(
    ("name", "document"),
    [
        ("running-without-pid", _task_owner_document(status="running")),
        ("null-pid-and-empty-owner", _task_owner_document(status="running", pid=None, owner={})),
        ("unobserved-without-task-pid", _task_owner_document(status="queued")),
        ("terminal-with-incomplete-owner", _task_owner_document(status="completed", exit_code=0, owner={"pid": 1234})),
    ],
)
def test_recovery_refuses_ambiguous_task_ownership_without_mutation(
    tmp_path: Path, name: str, document: bytes
) -> None:
    root = tmp_path / name
    root.mkdir()
    (root / ".harflow-store.json").write_bytes(_marker(product="foreign"))
    (root / ".harflow-store.lock").touch()
    task_path = root / "tasks" / "owner.meta.json"
    task_path.parent.mkdir()
    task_path.write_bytes(document)

    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    before = {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()}
    with pytest.raises(StateOwnershipError) as caught:
        state_recovery.recover_state(
            resolve_layout(root=root),
            operation_id=proposal["operation_id"],
            confirmation_token=proposal["confirmation_token"],
        )

    assert caught.value.code == "state_busy"
    assert getattr(caught.value, "reason", None) == "ambiguous_owner"
    assert {path.relative_to(root): path.read_bytes() for path in root.rglob("*") if path.is_file()} == before
    assert not Path(proposal["backup_path"]).exists()


def test_terminal_task_without_owner_can_be_recovered_with_arbitrary_archived_blob(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / ".harflow-store.json").write_bytes(_marker(product="foreign"))
    (root / ".harflow-store.lock").touch()
    task_path = root / "tasks" / "finished.meta.json"
    task_path.parent.mkdir()
    task_path.write_bytes(_task_owner_document(status="completed", exit_code=0))
    task_bytes = task_path.read_bytes()
    archived = root / "tasks" / "archived-output.bin"
    archived.write_bytes(b"opaque archived bytes")

    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    result = state_recovery.recover_state(
        resolve_layout(root=root),
        operation_id=proposal["operation_id"],
        confirmation_token=proposal["confirmation_token"],
    )

    assert result["status"] == "completed"
    backup = Path(result["backup_path"]) / "data"
    assert (backup / "tasks" / "finished.meta.json").read_bytes() == task_bytes
    assert (backup / "tasks" / "archived-output.bin").read_bytes() == b"opaque archived bytes"


def test_recovery_archives_all_bytes_without_deleting_original_data(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / ".harflow-store.json").write_bytes(_marker(product="foreign"))
    data = {"a.bin": b"\x00\x01", "nested/item": b"keep me"}
    for name, value in data.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)

    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    result = state_recovery.recover_state(
        resolve_layout(root=root),
        operation_id=proposal["operation_id"],
        confirmation_token=proposal["confirmation_token"],
    )

    assert result["status"] == "completed"
    assert result["operation_id"] == proposal["operation_id"]
    assert result["backup_id"] == result["operation_id"]
    backup = Path(result["backup_path"])
    assert backup.is_dir()
    assert (backup / "data" / ".harflow-store.json").read_bytes() == _marker(product="foreign")
    for name, value in data.items():
        assert (backup / "data" / name).read_bytes() == value
    assert (root / ".harflow-store.json").exists()
    assert (root / ".harflow-store.json").read_bytes() != _marker(product="foreign")


def test_recovery_repeat_reconciles_same_receipt_without_moving_new_writes(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / ".harflow-store.json").write_bytes(_marker(product="foreign"))
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    first = state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    new_file = root / "new-same-line-write"
    new_file.write_bytes(b"must survive response loss")

    second = state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])

    assert second == first
    assert new_file.read_bytes() == b"must survive response loss"


def test_stale_confirmation_is_rejected_after_root_inventory_changes(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"one")
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    (root / "foreign.data").write_bytes(b"two")

    with pytest.raises(StateOwnershipError) as caught:
        state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert getattr(caught.value, "code", None) == "state_recovery_stale"
    assert getattr(caught.value, "reason", None) == "stale_confirmation"


def test_recovery_refuses_competing_process_lock(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"keep")
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    lock = root / ".harflow-store.lock"
    lock.touch()
    holder_code = "import fcntl,sys,time; f=open(sys.argv[1],'r+'); fcntl.flock(f, fcntl.LOCK_EX); print('ready',flush=True); sys.stdin.read(1)"
    holder = subprocess.Popen([sys.executable, "-c", holder_code, str(lock)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout is not None and holder.stdout.readline().strip() == "ready"
        with pytest.raises(StateOwnershipError) as caught:
            state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
        assert caught.value.code == "state_busy"
        assert not Path(proposal["backup_path"]).exists()
    finally:
        assert holder.stdin is not None
        holder.stdin.write("x")
        holder.stdin.flush()
        holder.wait(timeout=3)


def test_recovery_rejects_symlink_root_as_unsafe(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    root = tmp_path / "state-link"
    root.symlink_to(target, target_is_directory=True)
    result = state_recovery.inspect_state(resolve_layout(root=root))
    assert result["reason"] == "unsafe_root"
    assert result["recovery_supported"] is False


def test_backup_collision_fails_without_overwriting_existing_sibling(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"keep")
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    collision = Path(proposal["backup_path"])
    collision.mkdir()
    sentinel = collision / "sentinel"
    sentinel.write_bytes(b"do not overwrite")

    with pytest.raises(StateOwnershipError) as caught:
        state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert caught.value.code == "state_recovery_unsafe"
    assert getattr(caught.value, "reason", None) == "backup_conflict"
    assert sentinel.read_bytes() == b"do not overwrite"
    assert (root / "foreign.data").read_bytes() == b"keep"


def test_move_failure_leaves_journal_and_same_operation_resumes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"keep")
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    original_rename = os.rename
    failed = False

    def fail_once(source, target):
        nonlocal failed
        if not failed and Path(source).name == "foreign.data":
            failed = True
            raise OSError("injected move failure")
        return original_rename(source, target)

    monkeypatch.setattr(state_recovery.os, "rename", fail_once)
    with pytest.raises(StateOwnershipError) as caught:
        state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert caught.value.code == "state_recovery_failed"
    assert (root / state_recovery.JOURNAL_NAME).exists()
    monkeypatch.setattr(state_recovery.os, "rename", original_rename)
    result = state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert result["status"] == "completed"


def test_final_marker_failure_resumes_without_new_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"keep")
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    original_write = state_recovery._write
    failed = False

    def fail_once(path, document):
        nonlocal failed
        if not failed and Path(path).name == state_recovery.RECEIPT_NAME:
            failed = True
            raise OSError("injected receipt publication failure")
        return original_write(path, document)

    monkeypatch.setattr(state_recovery, "_write", fail_once)
    with pytest.raises(StateOwnershipError):
        state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    new_file = root / "new-after-failed-commit"
    new_file.write_bytes(b"preserve")
    monkeypatch.setattr(state_recovery, "_write", original_write)
    result = state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert result["status"] == "completed"
    assert new_file.read_bytes() == b"preserve"


def test_new_operation_archives_completed_prior_journal_and_old_repeat_is_read_only(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"first")
    first_proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    first = state_recovery.recover_state(resolve_layout(root=root), operation_id=first_proposal["operation_id"], confirmation_token=first_proposal["confirmation_token"])
    old_repeat_file = root / "old-token-write"
    old_repeat_file.write_bytes(b"retain")
    assert state_recovery.recover_state(resolve_layout(root=root), operation_id=first_proposal["operation_id"], confirmation_token=first_proposal["confirmation_token"]) == first

    # Simulate a separately qualified incompatible store appearing after the
    # first operation; the old completed receipt remains a read-only history.
    (root / ".harflow-store.json").write_bytes(_marker(product="foreign"))
    (root / "second.data").write_bytes(b"second")
    second_proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    assert second_proposal["operation_id"] != first_proposal["operation_id"]
    second = state_recovery.recover_state(resolve_layout(root=root), operation_id=second_proposal["operation_id"], confirmation_token=second_proposal["confirmation_token"])
    assert second["operation_id"] != first["operation_id"]
    assert (Path(second["backup_path"]) / "data" / old_repeat_file.name).read_bytes() == b"retain"
    assert (Path(second["backup_path"]) / "previous-recovery.json").exists()


def test_pending_journal_publication_replays_on_explicit_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"keep")
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    original_replace = state_recovery.os.replace
    failed = False

    def fail_once(source, target):
        nonlocal failed
        if not failed and Path(target).name == state_recovery.JOURNAL_NAME:
            failed = True
            raise OSError("injected journal publication interruption")
        return original_replace(source, target)

    monkeypatch.setattr(state_recovery.os, "replace", fail_once)
    with pytest.raises(StateOwnershipError):
        state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert (root / state_recovery.JOURNAL_PENDING).exists()
    monkeypatch.setattr(state_recovery.os, "replace", original_replace)
    result = state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert result["status"] == "completed"


def test_backup_manifest_publication_interruption_replays_on_same_operation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.data").write_bytes(b"keep")
    proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    original_replace = state_recovery.os.replace
    failed = False

    def fail_once(source, target):
        nonlocal failed
        if not failed and Path(target).name == state_recovery.MANIFEST_NAME and Path(source).name.endswith(".pending"):
            failed = True
            raise OSError("injected backup manifest publication interruption")
        return original_replace(source, target)

    monkeypatch.setattr(state_recovery.os, "replace", fail_once)
    with pytest.raises(StateOwnershipError):
        state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    monkeypatch.setattr(state_recovery.os, "replace", original_replace)
    result = state_recovery.recover_state(resolve_layout(root=root), operation_id=proposal["operation_id"], confirmation_token=proposal["confirmation_token"])
    assert result["status"] == "completed"
    assert (Path(result["backup_path"]) / "data" / "foreign.data").read_bytes() == b"keep"


def test_distinct_operation_resumes_after_previous_journal_archive_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "first.data").write_bytes(b"first")
    first_proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    state_recovery.recover_state(resolve_layout(root=root), operation_id=first_proposal["operation_id"], confirmation_token=first_proposal["confirmation_token"])
    (root / ".harflow-store.json").write_bytes(_marker(product="foreign"))
    (root / "second.data").write_bytes(b"second")
    second_proposal = state_recovery.inspect_state(resolve_layout(root=root))["recovery"]
    original_sync = state_recovery._sync
    failed = False

    def fail_once(directory):
        nonlocal failed
        if not failed and Path(directory).name.startswith("state.harflow-backup-"):
            backup = Path(directory)
            if (backup / "previous-recovery.json").exists():
                failed = True
                raise OSError("injected previous journal archive interruption")
        return original_sync(directory)

    monkeypatch.setattr(state_recovery, "_sync", fail_once)
    with pytest.raises(StateOwnershipError):
        state_recovery.recover_state(resolve_layout(root=root), operation_id=second_proposal["operation_id"], confirmation_token=second_proposal["confirmation_token"])
    monkeypatch.setattr(state_recovery, "_sync", original_sync)
    result = state_recovery.recover_state(resolve_layout(root=root), operation_id=second_proposal["operation_id"], confirmation_token=second_proposal["confirmation_token"])
    assert result["status"] == "completed"
    assert (Path(result["backup_path"]) / "previous-recovery.json").exists()


@pytest.mark.parametrize("corruption", ["task", "sqlite"])
def test_cross_line_inspection_refuses_corrupt_private_state_without_mutation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corruption: str) -> None:
    root = tmp_path / "state"
    root.mkdir()
    marker = _marker(version="0.0.1")
    marker_path = root / ".harflow-store.json"
    marker_path.write_bytes(marker)
    (root / ".harflow-store.lock").touch()
    if corruption == "task":
        tasks = root / "tasks"
        tasks.mkdir()
        (tasks / "bad.meta.json").write_bytes(b"{not-json")
    else:
        (root / "notifications.sqlite3").write_bytes(b"not sqlite")
    before = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    monkeypatch.setattr(state_recovery, "PRODUCT_VERSION", "0.1.0")
    monkeypatch.setattr(state_ownership, "PRODUCT_VERSION", "0.1.0")
    monkeypatch.setattr(state_compatibility, "PRODUCT_VERSION", "0.1.0")

    result = state_recovery.inspect_state(resolve_layout(root=root))

    assert result["status"] == "incompatible"
    assert result["reason"] == "migration_preflight_failed"
    assert result["recovery"]["operation_id"]
    assert {path: path.read_bytes() for path in root.rglob("*") if path.is_file()} == before
    assert not (root / state_recovery.JOURNAL_NAME).exists()
