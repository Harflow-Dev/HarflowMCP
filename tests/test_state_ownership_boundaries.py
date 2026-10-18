"""State ownership must guard direct storage boundaries and outlive inner resources."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import hashlib
import fcntl
import os
from pathlib import Path
import selectors
import subprocess
import sys

import pytest

from harflow_mcp import artifacts, approvals, catalog, control, events, followups, inbox, notifications
from harflow_mcp import retention, scratch, store, workflows
from harflow_mcp import state_ownership as ownership
from harflow_mcp.state_access import open_descriptor
from harflow_mcp.state_layout import resolve_layout


def _layout(root: Path):
    return resolve_layout(root=root)


def _marker(product: str = "harflow") -> bytes:
    return (json.dumps({
        "product": product,
        "format": "harflow.store",
        "v": 1,
        "product_version": "1.0.0",
        "layout_version": 1,
    }, sort_keys=True) + "\n").encode()


def _record() -> store.TaskRecord:
    return store.TaskRecord("task-1", "codex", "session", "/repo", "2026-10-19T00:00:00+00:00")


def _enter_once(context):
    with context:
        pass


def _exclusive_once(layout):
    with ownership.exclusive_session(_layout(layout), timeout=0.05):
        pass


def _operations(root: Path):
    tasks = root / "tasks"
    return {
        "store-write": lambda: store.write(tasks, _record()),
        "store-read": lambda: store.read(tasks, "task-1"),
        "store-list": lambda: store.list_page(tasks),
        "catalog-connect": lambda: catalog.Catalog(tasks, store.RECORD_SUFFIX, layout=_layout(root)).connect(),
        "workflow-store": lambda: workflows.WorkflowStore(root),
        "notifications": lambda: notifications.status(tasks, "task-1"),
        "retention": lambda: retention.sweep(tasks, 1, datetime.now(timezone.utc)),
        "scratch": lambda: scratch.create(tasks, "task-1"),
        "events": lambda: events.EventLog(root / "task-1.events.jsonl", "task-1", layout=_layout(root)),
        "control-lock": lambda: _enter_once(control.session_lock_sync(tasks, "session")),
        "inbox-lock": lambda: _enter_once(_inbox_lock_context(tasks)),
        "followups": lambda: followups.view(tasks, "task-1"),
        "approvals": lambda: approvals.rows(tasks, "task-1"),
        "artifacts": lambda: artifacts.summary(tasks, "task-1"),
    }


def _inbox_lock_context(tasks: Path):
    class InboxLock:
        def __enter__(self):
            self.fd = inbox.lock_sync(tasks, "task-1")
            return self.fd

        def __exit__(self, *exc):
            inbox.unlock(self.fd)

    return InboxLock()


def _snapshot(root: Path) -> dict[str, object]:
    snapshot = {}
    for path in root.rglob("*"):
        relative = str(path.relative_to(root))
        if path.is_symlink():
            snapshot[relative] = "symlink:" + str(path.readlink())
        elif path.is_dir():
            snapshot[relative] = "directory"
        elif path.is_file():
            snapshot[relative] = path.read_bytes()
    return snapshot


@pytest.mark.parametrize("operation", sorted(_operations(Path("/tmp/placeholder"))))
@pytest.mark.parametrize("root_kind", ["foreign-marker", "nonempty-unmarked"])
def test_direct_storage_boundaries_refuse_before_any_write(tmp_path, operation, root_kind):
    root = tmp_path / "state"
    root.mkdir()
    if root_kind == "foreign-marker":
        (root / ownership.MARKER_NAME).write_bytes(_marker("other-product"))
    else:
        (root / "legacy-state").write_bytes(b"historical bytes")
    before = _snapshot(root)

    with pytest.raises(ownership.StateOwnershipError):
        _operations(root)[operation]()

    assert _snapshot(root) == before
    assert not (root / ownership.LOCK_NAME).exists()
    assert not (root / "tasks").exists()
    assert not (root / "workflow-runs").exists()
    assert not (root / "notifications.sqlite3").exists()
    assert not (root / "scratch").exists()


def test_active_capability_cannot_reinitialize_replaced_root(tmp_path):
    """A live operation must reject a replaced root instead of initializing its new inode."""
    root = tmp_path / "state"
    layout = _layout(root)
    record = _record()
    with ownership.state_session(layout):
        store.write(layout.tasks_directory, record)
        old_bytes = _snapshot(root)
        moved = tmp_path / "old-state"
        root.rename(moved)
        root.mkdir()
        with pytest.raises(ownership.StateIncompatible):
            store.write(layout.tasks_directory, store.TaskRecord(
                "task-2", "codex", "session", "/repo", "2026-10-19T00:00:01+00:00"
            ))
        assert _snapshot(root) == {}
        assert _snapshot(moved) == old_bytes


def test_active_capability_refuses_root_missing_after_rename(tmp_path):
    """A missing root during an active operation must not be recreated by a write."""
    root = tmp_path / "state"
    layout = _layout(root)
    with ownership.state_session(layout):
        store.write(layout.tasks_directory, _record())
        moved = tmp_path / "old-state"
        root.rename(moved)
        with pytest.raises(ownership.StateIncompatible):
            store.write(layout.tasks_directory, store.TaskRecord(
                "task-2", "codex", "session", "/repo", "2026-10-19T00:00:01+00:00"
            ))
        assert not root.exists()
        assert moved.exists()


def test_event_log_retains_ownership_until_close_after_outer_session(tmp_path):
    root = tmp_path / "state"
    path = root / "task-1.events.jsonl"
    with ownership.state_session(_layout(root)):
        log = events.EventLog(path, "task-1", layout=_layout(root))
        assert log.write("notice", {"message": "before"})

    try:
        with pytest.raises(ownership.StateBusy):
            _exclusive_once(root)
        assert log.write("notice", {"message": "after"})
        assert path.read_text().count("\n") == 2
    finally:
        log.close()
    assert log._handle is None
    with ownership.exclusive_session(_layout(root)):
        pass


def test_catalog_connection_retains_lock_and_sqlite_after_outer_session(tmp_path):
    root = tmp_path / "state"
    directory = root / "tasks"
    with ownership.state_session(_layout(root)):
        connection = catalog.Catalog(directory, store.RECORD_SUFFIX, layout=_layout(root)).connect()
        connection.execute("INSERT INTO pending VALUES ('late')")
        connection.commit()

    try:
        with pytest.raises(ownership.StateBusy):
            _exclusive_once(root)
        assert connection.execute("SELECT id FROM pending").fetchone()[0] == "late"
    finally:
        connection.close()
    assert connection._catalog_lock is None
    with ownership.exclusive_session(_layout(root)):
        pass


def test_control_lock_resource_survives_outer_session_until_close(tmp_path):
    root = tmp_path / "state"
    tasks = root / "tasks"
    with ownership.state_session(_layout(root)):
        lock = control.session_lock_sync(tasks, "session")
        lock.__enter__()

    try:
        with pytest.raises(ownership.StateBusy):
            _exclusive_once(root)
    finally:
        lock.__exit__(None, None, None)
    assert (root / "sessions" / "session.lock").exists()
    with ownership.exclusive_session(_layout(root)):
        pass


def test_event_page_consumption_is_inside_owned_operation(tmp_path):
    root = tmp_path / "state"
    path = root / "task-1.events.jsonl"
    with ownership.state_session(_layout(root)):
        log = events.EventLog(path, "task-1", layout=_layout(root))
        assert log.write("notice", {"message": "page"})
        log.close()
    generator = events._iter_events_forward(path, [], layout=_layout(root))
    assert next(generator)["kind"] == "notice"
    try:
        with pytest.raises(ownership.StateBusy):
            _exclusive_once(root)
    finally:
        generator.close()
    with ownership.exclusive_session(_layout(root)):
        pass


def test_event_generator_retains_lease_without_restoring_closed_context(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    path = root / "task-1.events.jsonl"
    with ownership.state_session(layout) as parent:
        log = events.EventLog(path, "task-1", layout=layout)
        assert log.write("notice", {"message": "first"})
        assert log.write("notice", {"message": "second"})
        log.close()
        generator = events._iter_events_forward(path, [], layout=layout)
        assert ownership.assert_owned(layout) is parent
        assert next(generator)["message"] == "first"
        assert ownership.assert_owned(layout) is parent
    with pytest.raises(ownership.StateIncompatible):
        ownership.assert_owned(layout)
    try:
        with pytest.raises(ownership.StateBusy):
            _exclusive_once(root)
        assert next(generator)["message"] == "second"
        with pytest.raises(ownership.StateIncompatible):
            ownership.assert_owned(layout)
    finally:
        generator.close()
    with ownership.exclusive_session(layout):
        pass


def test_event_generator_primary_error_is_not_masked_by_cleanup(tmp_path, monkeypatch):
    """A reader failure remains the primary exception while its retained lease is released."""
    root = tmp_path / "state"
    layout = _layout(root)
    path = root / "task-1.events.jsonl"
    with ownership.state_session(layout):
        log = events.EventLog(path, "task-1", layout=layout)
        assert log.write("notice", {"message": "error"})
        log.close()

    def explode(*args, **kwargs):
        lock = root / ownership.LOCK_NAME
        lock.unlink()
        lock.touch()
        raise RuntimeError("primary reader failure")

    monkeypatch.setattr(events, "_iter_open_log", explode)
    with pytest.raises(RuntimeError, match="primary reader failure"):
        list(events._iter_events_forward(path, [], layout=layout))
    with ownership.exclusive_session(layout):
        pass


def test_event_generator_invalidated_lock_still_closes_raw_reader(tmp_path, monkeypatch):
    """Invalid lifetime validation must not prevent the underlying reader from closing."""
    root = tmp_path / "state"
    layout = _layout(root)
    path = root / "task-1.events.jsonl"
    with ownership.state_session(layout):
        path.write_text("event\n")
    closed = []

    def reader(*args, **kwargs):
        try:
            yield {"kind": "notice", "seq": 1}
        finally:
            closed.append(True)

    monkeypatch.setattr(events, "_iter_open_log", reader)
    generator = events._iter_events_forward(path, [], layout=layout)
    assert next(generator)["seq"] == 1
    lock = root / ownership.LOCK_NAME
    lock.unlink()
    lock.touch()
    with pytest.raises(ownership.StateIncompatible):
        generator.close()
    assert closed == [True]


def test_native_event_sweep_skips_contended_run_and_continues(tmp_path):
    now = datetime.now(timezone.utc)
    root = tmp_path / "state"
    tasks = root / "tasks"
    runs = root / "workflow-runs"
    layout = _layout(root)
    old_finished = (now.timestamp() - 40 * 86400)
    with ownership.state_session(layout):
        pass
    runs.mkdir(parents=True)
    for run_id, task_id in (("run-a", "native-a"), ("run-b", "native-b")):
        (runs / f"{run_id}.json").write_text(json.dumps({
            "workflow_run_id": run_id,
            "status": "completed",
            "activations": [{"tasks": [{
                "task_id": task_id,
                "status": "completed",
                "execution_kind": "native_subagent",
                "native_terminal": True,
                "dispatch_stage": "child_settled",
                "finished_at": old_finished,
            }]}],
        }))
    tasks.mkdir(parents=True)
    paths = {task_id: tasks / f"{task_id}.events.jsonl" for task_id in ("native-a", "native-b")}
    for path in paths.values():
        path.write_text("event\n")
        os.utime(path, (old_finished, old_finished))
    held_path = root / f".workflow-{hashlib.sha256(b'run:run-a').hexdigest()}.lock"
    held_path.parent.mkdir(parents=True, exist_ok=True)
    held_fd = os.open(held_path, os.O_CREAT | os.O_RDWR, 0o600)
    fcntl.flock(held_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with ownership.state_session(layout):
            stats = {}
            retention._sweep_native_events(tasks, 30, now, stats)
        assert paths["native-a"].exists()
        assert not paths["native-b"].exists()
        assert stats["deleted_native_events"] == 1
    finally:
        fcntl.flock(held_fd, fcntl.LOCK_UN)
        os.close(held_fd)


def test_notification_database_retains_ownership_until_connection_close(tmp_path):
    root = tmp_path / "state"
    tasks = root / "tasks"
    layout = _layout(root)
    with ownership.state_session(layout):
        database = notifications._db(tasks)
        assert database.execute("SELECT 1").fetchone()[0] == 1
    try:
        with pytest.raises(ownership.StateBusy):
            _exclusive_once(root)
        assert database.execute("SELECT 1").fetchone()[0] == 1
    finally:
        database.close()
    with ownership.exclusive_session(layout):
        pass


def test_catalog_close_preserves_outer_contextvar_and_releases_only_its_refs(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    with ownership.state_session(layout) as parent:
        connection = catalog.Catalog(root / "tasks", store.RECORD_SUFFIX, layout=layout).connect()
        assert ownership.assert_owned(layout) is parent
        try:
            connection.close()
            assert ownership.assert_owned(layout) is parent
        finally:
            connection.close()
    with pytest.raises(ownership.StateIncompatible):
        ownership.assert_owned(layout)


@pytest.mark.parametrize("close_order", [(0, 1), (1, 0)])
def test_two_catalog_connections_keep_outer_context_and_refs_until_both_close(
    tmp_path, close_order
):
    root = tmp_path / "state"
    layout = _layout(root)
    directory = root / "tasks"
    with ownership.state_session(layout) as parent:
        connections = [
            catalog.Catalog(directory, store.RECORD_SUFFIX, layout=layout).connect()
            for _ in range(2)
        ]
        assert ownership.assert_owned(layout) is parent
        for connection in connections:
            connection._state_lease.validate()
    with pytest.raises(ownership.StateIncompatible):
        ownership.assert_owned(layout)
    try:
        connections[close_order[0]].close()
        connections[close_order[1]]._state_lease.validate()
        with pytest.raises(ownership.StateBusy):
            _exclusive_once(root)
        connections[close_order[1]].close()
        with ownership.exclusive_session(layout):
            pass
    finally:
        for connection in connections:
            connection.close()
    with pytest.raises(ownership.StateIncompatible):
        ownership.assert_owned(layout)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is unavailable")
def test_forked_catalog_close_cannot_unlock_parent_listing_lock(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    directory = root / "tasks"
    lock_path = directory / ".listing.lock"
    child_read, child_write = os.pipe()
    child_pid = None
    connection = None
    try:
        with ownership.state_session(layout):
            connection = catalog.Catalog(directory, store.RECORD_SUFFIX, layout=layout).connect()
            child_pid = os.fork()
            if child_pid == 0:
                os.close(child_read)
                try:
                    connection.close()
                    os.write(child_write, b"closed\n")
                except BaseException as exc:
                    os.write(child_write, (f"error:{type(exc).__name__}\n").encode())
                finally:
                    os.close(child_write)
                    os._exit(0)
            os.close(child_write)
            selector = selectors.DefaultSelector()
            selector.register(child_read, selectors.EVENT_READ)
            try:
                events = selector.select(2.0)
                assert events, "fork child did not finish bounded close"
                payload = os.read(child_read, 200)
                assert payload.splitlines() == [b"closed"]
            finally:
                selector.close()
                os.close(child_read)
            _, status = os.waitpid(child_pid, 0)
            child_pid = None
            assert os.waitstatus_to_exitcode(status) == 0

            probe = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "import fcntl,sys; fd=open(sys.argv[1], 'a+b'); "
                    "\ntry: fcntl.flock(fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                    "except BlockingIOError: print('busy')\nelse: print('acquired')\n",
                    str(lock_path),
                ],
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            assert probe.returncode == 0
            assert probe.stdout.strip() == "busy"
            connection.close()
            connection = None
        with ownership.exclusive_session(layout, timeout=0.2):
            pass
    finally:
        if child_pid is not None:
            os.kill(child_pid, 9)
            os.waitpid(child_pid, 0)
        if child_read is not None:
            try:
                os.close(child_read)
            except OSError:
                pass
        if child_write is not None:
            try:
                os.close(child_write)
            except OSError:
                pass
        if connection is not None:
            connection.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is unavailable")
def test_forked_owned_descriptor_close_does_not_unlock_parent(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    directory = root / "tasks"
    resource_path = directory / ".descriptor.lock"
    child_read, child_write = os.pipe()
    child_pid = None
    descriptor = None
    try:
        with ownership.state_session(layout):
            directory.mkdir(parents=True, exist_ok=True)
            descriptor = open_descriptor(resource_path, os.O_CREAT | os.O_RDWR, layout=layout)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            child_pid = os.fork()
            if child_pid == 0:
                os.close(child_read)
                try:
                    descriptor.close(unlock=True)
                    os.write(child_write, b"closed\n")
                except BaseException as exc:
                    os.write(child_write, (f"error:{type(exc).__name__}\n").encode())
                finally:
                    os.close(child_write)
                    os._exit(0)
            os.close(child_write)
            selector = selectors.DefaultSelector()
            selector.register(child_read, selectors.EVENT_READ)
            try:
                assert selector.select(2.0), "fork child did not finish bounded close"
                assert os.read(child_read, 200).splitlines() == [b"closed"]
            finally:
                selector.close()
                os.close(child_read)
            _, status = os.waitpid(child_pid, 0)
            child_pid = None
            assert os.waitstatus_to_exitcode(status) == 0
            probe = subprocess.run(
                [sys.executable, "-c", "import fcntl,sys; f=open(sys.argv[1], 'a+b');\ntry: fcntl.flock(f.fileno(), fcntl.LOCK_EX|fcntl.LOCK_NB)\nexcept BlockingIOError: print('busy')\nelse: print('acquired')", str(resource_path)],
                capture_output=True, text=True, timeout=2.0, check=False,
            )
            assert probe.returncode == 0 and probe.stdout.strip() == "busy"
            descriptor.close(unlock=True)
            descriptor = None
        with ownership.exclusive_session(layout, timeout=0.2):
            pass
    finally:
        if descriptor is not None:
            descriptor.close(unlock=True)
        if child_pid is not None:
            os.kill(child_pid, 9)
            os.waitpid(child_pid, 0)


@pytest.mark.parametrize("failure", ["regular", "fstat"])
def test_kernel_identity_oserror_is_reported_as_state_incompatible(tmp_path, monkeypatch, failure):
    root = tmp_path / "state"
    layout = _layout(root)
    with ownership.state_session(layout) as lease:
        def fail(*args, **kwargs):
            raise OSError("identity unavailable")

        with monkeypatch.context() as patch:
            if failure == "regular":
                patch.setattr(ownership, "_regular", fail)
            else:
                patch.setattr(ownership.os, "fstat", fail)
            for operation in (
                lease.validate,
                ownership.current_owned,
                lambda: ownership.assert_owned(layout),
                lease.retain,
            ):
                with pytest.raises(ownership.StateIncompatible):
                    operation()
        # Restore the kernel identity seam before normal context cleanup.
        lease.validate()
