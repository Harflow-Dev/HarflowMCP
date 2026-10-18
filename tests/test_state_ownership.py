"""Contract and lifecycle tests for cooperative state-root ownership."""
from __future__ import annotations

import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
import inspect
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import threading
import time

import pytest

from harflow_mcp import state_ownership as ownership
from harflow_mcp import control, tasks
from harflow_mcp import events
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import PRODUCT, PRODUCT_VERSION


def _layout(root: Path):
    return resolve_layout(root=root)


def _marker(*, product=PRODUCT, product_version=PRODUCT_VERSION, v=1, layout_version=1) -> bytes:
    return (json.dumps({
        "product": product,
        "format": "harflow.store",
        "v": v,
        "product_version": product_version,
        "layout_version": layout_version,
    }, sort_keys=True) + "\n").encode()


def _write_marker(root: Path, raw: bytes = _marker()) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / ownership.MARKER_NAME).write_bytes(raw)


def _assert_refused_without_lock(root: Path, raw: bytes) -> None:
    marker = root / ownership.MARKER_NAME
    marker.write_bytes(raw)
    before = marker.read_bytes()
    with pytest.raises(ownership.StateIncompatible):
        with ownership.state_session(_layout(root), timeout=0.05):
            pass
    assert marker.read_bytes() == before
    assert not (root / ownership.LOCK_NAME).exists()


def _subprocess(code: str, *args: str, timeout: float = 5.0) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    for key in ("HARFLOW_INTEGRATION", "PB_INTEGRATION", "HARFLOW_CLI_INTEGRATION", "PB_CLI_INTEGRATION"):
        env.pop(key, None)
    return subprocess.run(
        [sys.executable, "-c", code, *args],
        text=True,
        capture_output=True,
        env=env,
        timeout=timeout,
        check=False,
    )


def _exclusive_success(layout) -> None:
    deadline = time.monotonic() + 2.0
    while True:
        try:
            with ownership.exclusive_session(layout, timeout=0.1):
                break
        except ownership.StateBusy:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)


def _readline_bounded(stream, timeout: float) -> str:
    selector = selectors.DefaultSelector()
    selector.register(stream, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout
    data = bytearray()
    while time.monotonic() < deadline:
        if not selector.select(deadline - time.monotonic()):
            break
        chunk = os.read(stream.fileno(), 1)
        if not chunk:
            break
        data.extend(chunk)
        if data.endswith(b"\n"):
            return data.decode().rstrip("\r\n")
    raise AssertionError("subprocess handshake timed out")


def _cleanup_processes(processes, release: Path | None = None) -> None:
    if release is not None:
        release.touch()
    for process in processes:
        if process.poll() is None:
            process.kill()
    for process in processes:
        try:
            process.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=2)


def test_fresh_empty_root_initializes_marker_and_stable_lock(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    with ownership.state_session(layout):
        assert json.loads((root / ownership.MARKER_NAME).read_text()) == {
            "format": "harflow.store",
            "layout_version": 1,
            "product": PRODUCT,
            "product_version": PRODUCT_VERSION,
            "v": 1,
        }
        assert (root / ownership.LOCK_NAME).is_file()
        assert not (root / ownership.PENDING_NAME).exists()
    assert (root / ownership.LOCK_NAME).exists()


@pytest.mark.parametrize("error_type", [ownership.StateOwnershipError, ownership.StateIncompatible, ownership.StateBusy])
def test_ownership_errors_do_not_claim_no_startup_proof(error_type):
    error = error_type("ownership failure")
    assert not getattr(error, "harflow_not_started", False)


@pytest.mark.parametrize("raw", [
    b"{not json}\n",
    b"x" * (ownership.MAX_MARKER_BYTES + 1),
    _marker(product="other"),
    _marker(v=True),
    _marker(layout_version=True),
    _marker(product_version="1.1.0"),
    _marker(product_version="1.0.0-rc.1"),
])
def test_rejected_marker_preserves_bytes_and_does_not_create_lock(tmp_path, raw):
    root = tmp_path / "state"
    root.mkdir()
    _assert_refused_without_lock(root, raw)


@pytest.mark.parametrize("version", ["1.0.1", "1.0.0+local.build"])
def test_same_major_minor_marker_versions_are_accepted(tmp_path, version):
    root = tmp_path / "state"
    _write_marker(root, _marker(product_version=version))
    (root / ownership.LOCK_NAME).touch()
    with ownership.state_session(_layout(root), timeout=0.2):
        pass


def test_nonempty_unmarked_root_is_refused_without_mutation(tmp_path):
    root = tmp_path / "state"
    root.mkdir()
    (root / ".legacy-state").write_bytes(b"preserve")
    with pytest.raises(ownership.StateIncompatible):
        with ownership.state_session(_layout(root), timeout=0.05):
            pass
    assert (root / ".legacy-state").read_bytes() == b"preserve"
    assert not (root / ownership.LOCK_NAME).exists()


def test_marked_root_missing_stable_lock_is_refused(tmp_path):
    root = tmp_path / "state"
    _write_marker(root)
    with pytest.raises(ownership.StateIncompatible):
        with ownership.state_session(_layout(root), timeout=0.05):
            pass
    assert not (root / ownership.LOCK_NAME).exists()


@pytest.mark.parametrize("name", [ownership.MARKER_NAME, ownership.LOCK_NAME, ownership.PENDING_NAME])
def test_symlink_or_nonregular_ownership_infrastructure_is_refused(tmp_path, name):
    root = tmp_path / "state"
    root.mkdir()
    target = tmp_path / "target"
    target.write_bytes(b"target")
    (root / name).symlink_to(target)
    if name != ownership.MARKER_NAME:
        (root / ownership.MARKER_NAME).write_bytes(_marker())
    with pytest.raises(ownership.StateIncompatible):
        with ownership.state_session(_layout(root), timeout=0.05):
            pass
    assert (root / name).is_symlink()


def test_crash_pending_marker_is_recovered_only_on_fresh_unmarked_root(tmp_path):
    root = tmp_path / "state"
    root.mkdir()
    pending = root / ownership.PENDING_NAME
    pending.write_bytes(b"partial publication")
    with ownership.state_session(_layout(root)):
        assert (root / ownership.MARKER_NAME).is_file()
        assert not pending.exists()


def test_first_start_race_rechecks_lock_created_during_preflight(tmp_path, monkeypatch):
    root = tmp_path / "state"
    real_empty = ownership._empty
    created = False

    def create_lock_during_listing(path):
        nonlocal created
        if not created:
            created = True
            (path / ownership.LOCK_NAME).touch(mode=0o600)
        return real_empty(path)

    monkeypatch.setattr(ownership, "_empty", create_lock_during_listing)
    with ownership.state_session(_layout(root), timeout=0.5):
        pass
    assert created
    assert (root / ownership.MARKER_NAME).exists()


def test_simultaneous_first_initializers_both_succeed(tmp_path):
    root = tmp_path / "state"
    started = tmp_path / "started"
    marker_seen = tmp_path / "marker-seen"
    published = tmp_path / "published"
    shared = tmp_path / "shared"
    proceed = tmp_path / "proceed"
    release = tmp_path / "release"
    started.mkdir()
    root.mkdir()
    (root / ownership.LOCK_NAME).touch(mode=0o600)
    published.mkdir()
    code = """
import os
import time
from pathlib import Path
from fcntl import LOCK_SH
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp import state_ownership as ownership
root = Path(os.sys.argv[1])
started = Path(os.sys.argv[2])
marker_seen = Path(os.sys.argv[3])
published = Path(os.sys.argv[4])
shared = Path(os.sys.argv[5])
proceed = Path(os.sys.argv[6])
release = Path(os.sys.argv[7])
role = os.sys.argv[8]
started.joinpath(role).touch()
marker_gate = marker_seen / role
marker_gate.parent.mkdir(exist_ok=True)
real_wait = ownership._wait
real_marker = ownership._marker
real_publish = ownership._publish
gated = False
def marker(path):
    value = real_marker(path)
    if not value and not marker_gate.exists():
        marker_gate.touch()
        while not proceed.exists():
            time.sleep(.005)
    return value
def publish(path):
    real_publish(path)
    published.joinpath(role).touch()
def wait(fd, mode, deadline):
    global gated
    real_wait(fd, mode, deadline)
    if published.joinpath(role).exists() and mode == LOCK_SH and not gated:
        gated = True
        shared.touch()
        while not release.exists():
            time.sleep(.005)
ownership._marker = marker
ownership._publish = publish
ownership._wait = wait
with ownership.state_session(resolve_layout(root=root), timeout=1):
    print('ready', flush=True)
"""
    child_env = os.environ.copy()
    child_env["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    for key in ("HARFLOW_INTEGRATION", "PB_INTEGRATION", "HARFLOW_CLI_INTEGRATION", "PB_CLI_INTEGRATION"):
        child_env.pop(key, None)
    processes = []
    try:
        first = subprocess.Popen([sys.executable, "-c", code, str(root), str(started), str(marker_seen), str(published), str(shared), str(proceed), str(release), "winner"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                 env=child_env)
        processes.append(first)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not (started / "winner").exists():
            time.sleep(.005)
        second = subprocess.Popen([sys.executable, "-c", code, str(root), str(started), str(marker_seen), str(published), str(shared), str(proceed), str(release), "loser"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                  env=child_env)
        processes.append(second)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not (started / "loser").exists():
            time.sleep(.005)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not ((marker_seen / "winner").exists() and (marker_seen / "loser").exists()):
            time.sleep(.005)
        assert (marker_seen / "winner").exists() and (marker_seen / "loser").exists(), "both initializers did not reach the prevalidation barrier"
        proceed.touch()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not shared.exists():
            time.sleep(.005)
        assert shared.exists(), "publisher did not reach the held-shared-lock barrier"
        publisher = first if (published / "winner").exists() else second
        joiner = second if publisher is first else first
        joiner_out, joiner_err = joiner.communicate(timeout=3)
        assert (joiner.returncode, joiner_out.strip()) == (0, "ready"), joiner_err
        release.touch()
        publisher_out, publisher_err = publisher.communicate(timeout=3)
        assert (publisher.returncode, publisher_out.strip()) == (0, "ready"), publisher_err
        assert (root / ownership.MARKER_NAME).exists()
    finally:
        _cleanup_processes(processes, release)


def test_exclusive_session_refuses_same_process_upgrade_and_other_process_waits(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    with ownership.state_session(layout):
        with pytest.raises(ownership.StateBusy):
            with ownership.exclusive_session(layout, timeout=0.05):
                pass
        code = """
from pathlib import Path
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import StateBusy, exclusive_session
try:
    with exclusive_session(resolve_layout(root=Path(__import__('sys').argv[1])), timeout=.1):
        print('unexpected', flush=True)
except StateBusy:
    print('busy', flush=True)
"""
        result = _subprocess(code, str(root), timeout=3)
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "busy"


def test_alias_shares_physical_lock_and_replacement_invalidates_old_capability(tmp_path):
    root = tmp_path / "state"
    alias = tmp_path / "alias"
    with ownership.state_session(_layout(root)) as old:
        alias.symlink_to(root, target_is_directory=True)
        with ownership.state_session(_layout(alias)):
            pass
        copied = old.retain()
    old_root = tmp_path / "old-root"
    root.rename(old_root)
    root.mkdir()
    with ownership.state_session(_layout(root)):
        with pytest.raises(ownership.StateIncompatible):
            ownership.assert_owned(_layout(old_root))
        with pytest.raises(ownership.StateIncompatible):
            copied._validate(_layout(root))
    copied.close()


def test_copied_context_fails_after_scope_then_reacquisition(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    with ownership.state_session(layout):
        copied = contextvars.copy_context()
        assert copied.run(ownership.assert_owned, layout)
    with ownership.state_session(layout):
        with pytest.raises(ownership.StateIncompatible):
            copied.run(ownership.assert_owned, layout)

    with ownership.state_session(layout):
        pass
    with pytest.raises(ownership.StateIncompatible):
        copied.run(ownership.assert_owned, layout)

    def reacquire_in_copied_context():
        with ownership.state_session(layout):
            assert ownership.assert_owned(layout)

    copied.run(reacquire_in_copied_context)
    with ownership.state_session(layout):
        assert ownership.assert_owned(layout)


@pytest.mark.asyncio
async def test_owned_job_releases_exactly_once_on_normal_exception_and_cancel(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    calls: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def normal():
        started.set()
        await release.wait()
        calls.append("normal")
        (root / "normal-write").write_text("done")
        return 3

    with ownership.state_session(layout) as parent:
        task = ownership.owned_job(layout, normal())
        await started.wait()
    with pytest.raises(ownership.StateBusy):
        ownership.exclusive_session(layout, timeout=0.05).__enter__()
    release.set()
    assert await task == 3
    assert (root / "normal-write").read_text() == "done"
    await asyncio.to_thread(_exclusive_success, layout)

    started = asyncio.Event()
    release = asyncio.Event()

    async def failed():
        started.set()
        await release.wait()
        calls.append("failed")
        (root / "exception-write").write_text("done")
        raise RuntimeError("boom")

    with ownership.state_session(layout):
        task = ownership.owned_job(layout, failed())
        await started.wait()
    with pytest.raises(ownership.StateBusy):
        ownership.exclusive_session(layout, timeout=0.05).__enter__()
    release.set()
    with pytest.raises(RuntimeError, match="boom"):
        await task
    assert (root / "exception-write").read_text() == "done"
    await asyncio.to_thread(_exclusive_success, layout)

    started = asyncio.Event()

    async def blocked():
        started.set()
        await asyncio.Event().wait()

    with ownership.state_session(layout):
        coroutine = blocked()
        task = ownership.owned_job(layout, coroutine)
        task.cancel()
    assert not started.is_set()
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CREATED
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
    await asyncio.to_thread(_exclusive_success, layout)
    assert not ownership._jobs


@pytest.mark.asyncio
async def test_owned_job_creation_failure_closes_unstarted_coroutine(tmp_path, monkeypatch):
    root = tmp_path / "state"
    layout = _layout(root)
    async with _async_session(layout):
        closed = False

        class Probe:
            def __await__(self):
                yield

            def close(self):
                nonlocal closed
                closed = True

        monkeypatch.setattr(asyncio, "create_task", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("schedule")))
        with pytest.raises(RuntimeError, match="schedule"):
            ownership.owned_job(layout, Probe())
        assert closed


@pytest.mark.asyncio
async def test_cancelled_worker_waiter_does_not_release_actual_thread_lease(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def worker():
        started.set()
        release.wait(2)
        (root / "late-write").write_text("done")
        finished.set()

    parent = ownership.state_session(layout)
    parent.__enter__()
    waiter = asyncio.create_task(ownership.run_owned_worker(layout, worker))
    await asyncio.to_thread(started.wait, 2)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    parent.__exit__(None, None, None)
    with pytest.raises(ownership.StateBusy):
        ownership.exclusive_session(layout, timeout=0.05).__enter__()
    release.set()
    assert await asyncio.to_thread(finished.wait, 2)
    assert (root / "late-write").read_text() == "done"
    await asyncio.to_thread(_exclusive_success, layout)


@pytest.mark.asyncio
async def test_queued_worker_survives_cancelled_waiter_and_parent_scope_close(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    blocker_started = threading.Event()
    blocker_release = threading.Event()
    worker_started = threading.Event()
    worker_release = threading.Event()
    worker_finished = threading.Event()
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(executor)

    def blocker():
        blocker_started.set()
        blocker_release.wait(2)

    def worker():
        with ownership.state_session(layout):
            worker_started.set()
            worker_release.wait(2)
            (root / "queued-late-write").write_text("done")
            worker_finished.set()

    blocker_task = asyncio.create_task(asyncio.to_thread(blocker))
    runner = waiter = None

    async def wait_event(event):
        deadline = time.monotonic() + 2
        while not event.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert event.is_set()

    try:
        await wait_event(blocker_started)
        with ownership.state_session(layout):
            runner = asyncio.create_task(ownership.run_owned_worker(layout, worker))
            async def wait_for_runner():
                return await asyncio.shield(runner)
            waiter = asyncio.create_task(wait_for_runner())
            await asyncio.sleep(0.05)
            assert not worker_started.is_set()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        blocker_release.set()
        await wait_event(worker_started)
        with pytest.raises(ownership.StateBusy):
            with ownership.exclusive_session(layout, timeout=0.05):
                pass
        worker_release.set()
        await wait_event(worker_finished)
        assert (root / "queued-late-write").read_text() == "done"
        assert await runner is None
        await asyncio.to_thread(_exclusive_success, layout)
    finally:
        blocker_release.set()
        worker_release.set()
        if waiter is not None and not waiter.done():
            waiter.cancel()
        if runner is not None and not runner.done():
            runner.cancel()
        if runner is not None:
            try:
                await runner
            except BaseException:
                pass
        if not blocker_task.done():
            await asyncio.wait_for(blocker_task, 2)
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_queued_worker_rejects_replaced_root_without_mutating_replacement(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    blocker_started = threading.Event()
    blocker_release = threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    asyncio.get_running_loop().set_default_executor(executor)

    def blocker():
        blocker_started.set()
        blocker_release.wait(2)

    def worker():
        with ownership.state_session(layout):
            raise AssertionError("replaced root was unexpectedly accepted")

    blocker_task = asyncio.create_task(asyncio.to_thread(blocker))
    runner = None
    async def wait_event(event):
        deadline = time.monotonic() + 2
        while not event.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        assert event.is_set()

    try:
        await wait_event(blocker_started)
        with ownership.state_session(layout):
            runner = asyncio.create_task(ownership.run_owned_worker(layout, worker))
            await asyncio.sleep(0.05)
            old_root = tmp_path / "old-state"
            root.rename(old_root)
            root.mkdir()
            before = tuple(sorted(path.name for path in root.iterdir()))
            blocker_release.set()
            with pytest.raises(ownership.StateIncompatible):
                await runner
            assert tuple(sorted(path.name for path in root.iterdir())) == before
    finally:
        blocker_release.set()
        if runner is not None and not runner.done():
            runner.cancel()
            try:
                await runner
            except BaseException:
                pass
        if not blocker_task.done():
            await asyncio.wait_for(blocker_task, 2)
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_schedule_automatic_retains_actual_continuation_until_late_write(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from harflow_mcp import task_continuation
    from harflow_mcp.backends.base import Accumulator

    root = tmp_path / "state"
    layout = _layout(root)
    directory = root / "tasks"
    with ownership.state_session(layout):
        pass
    directory.mkdir(parents=True, exist_ok=True)
    started = asyncio.Event()
    release = asyncio.Event()

    class Registry:
        def __init__(self):
            self.state_layout = layout
            self.log_dir = directory
            self._continuation_sources = set()
            self._continuations = set()
        def prune(self):
            pass
        def persist(self, task):
            pass

    registry = Registry()
    task = SimpleNamespace(
        task_id="source", backend="claude", status="completed", cancel_requested=False,
        acc=Accumulator(terminal={"subtype": "error_max_turns"}), exit_code=0,
        bridge_notices=[],
    )

    async def fake_continue(*args, **kwargs):
        started.set()
        await release.wait()
        (root / "continuation-final-write").write_text("done")

    monkeypatch.setattr(task_continuation, "continue_task", fake_continue)
    with ownership.state_session(layout):
        task_continuation.schedule_automatic(registry, task)
        await asyncio.wait_for(started.wait(), timeout=2)
    with pytest.raises(ownership.StateBusy):
        ownership.exclusive_session(layout, timeout=0.1).__enter__()
    release.set()
    deadline = asyncio.get_running_loop().time() + 2
    while not (root / "continuation-final-write").exists() and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert (root / "continuation-final-write").read_text() == "done"
    await asyncio.to_thread(_exclusive_success, layout)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork is unavailable")
def test_fork_child_invalidates_inherited_token_but_parent_lease_survives(tmp_path):
    root = tmp_path / "state"
    layout = _layout(root)
    read_fd, write_fd = os.pipe()
    with ownership.state_session(layout) as parent_lease:
        pid = os.fork()
        if pid == 0:
            parent_fd = parent_lease._handle.fd
            padding_source = os.open(os.devnull, os.O_RDONLY)
            os.dup2(padding_source, parent_fd, inheritable=False)
            if padding_source != parent_fd:
                os.close(padding_source)
            padding_fd = parent_fd
            try:
                try:
                    ownership.assert_owned(layout)
                except ownership.StateIncompatible:
                    os.write(write_fd, b"invalid\n")
                with ownership.state_session(layout) as child_lease:
                    extra_alias = None
                    assert child_lease._handle.fd != parent_fd
                    os.dup2(child_lease._handle.fd, parent_fd, inheritable=False)
                    extra_alias = parent_fd
                    try:
                        os.write(write_fd, b"acquired\n")
                        parent_lease.close()
                        os.fstat(parent_lease._handle.fd)
                        ownership.assert_owned(layout)
                        os.write(write_fd, b"token-safe\n")
                    finally:
                        if extra_alias is not None:
                            os.close(extra_alias)
                            padding_fd = None
            except BaseException as exc:
                os.write(write_fd, (f"acquire-error:{type(exc).__name__}\n").encode())
            finally:
                if padding_fd is not None:
                    os.close(padding_fd)
                os.close(write_fd)
                os._exit(0)
        os.close(write_fd)
        chunks = []
        selector = selectors.DefaultSelector()
        selector.register(read_fd, selectors.EVENT_READ)
        deadline = time.monotonic() + 3
        while True:
            events = selector.select(max(0, deadline - time.monotonic()))
            if not events:
                selector.close()
                os.close(read_fd)
                os.kill(pid, 9)
                os.waitpid(pid, 0)
                raise AssertionError("fork child did not report before deadline")
            chunk = os.read(read_fd, 100)
            if not chunk:
                break
            chunks.append(chunk)
        selector.close()
        payload = b"".join(chunks)
        os.close(read_fd)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
        assert payload.splitlines() == [b"invalid", b"acquired", b"token-safe"]
        assert ownership.assert_owned(layout)
        code = """
from pathlib import Path
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import StateBusy, exclusive_session
try:
    with exclusive_session(resolve_layout(root=Path(__import__('sys').argv[1])), timeout=.1):
        print('unexpected', flush=True)
except StateBusy:
    print('busy', flush=True)
"""
        result = _subprocess(code, str(root), timeout=3)
        assert result.returncode == 0
        assert result.stdout.strip() == "busy"


def test_exec_does_not_inherit_root_lease_descriptor(tmp_path):
    root = tmp_path / "state"
    code = """
import sys
from pathlib import Path
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import exclusive_session
print('ready', flush=True)
sys.stdin.read(1)
with exclusive_session(resolve_layout(root=Path(sys.argv[1])), timeout=.5):
    print('acquired', flush=True)
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).parents[1] / "src")
    child = None
    try:
        with ownership.state_session(_layout(root)):
            child = subprocess.Popen([sys.executable, "-c", code, str(root)], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False,
                                     env=env, close_fds=False, bufsize=0)
            assert _readline_bounded(child.stdout, 2) == "ready"
        child.stdin.write(b"x")
        child.stdin.flush()
        out, err = child.communicate(timeout=3)
        assert child.returncode == 0, err.decode()
        assert out.decode().strip().splitlines() == ["acquired"]
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.communicate(timeout=2)


def test_phase_retry_queued_from_worker_releases_ref_when_loop_closes(tmp_path, monkeypatch):
    """A worker callback queued after loop shutdown must not strand its retained root ref."""
    root = tmp_path / "state"
    layout = _layout(root)

    class ClosedLoop:
        def __init__(self):
            self.closed = False
            self.callbacks = []

        def call_soon_threadsafe(self, callback):
            if self.closed:
                raise RuntimeError("event loop is closed")
            self.callbacks.append(callback)
            return asyncio.Handle(callback, (), self)

        def get_debug(self):
            return False

        def is_closed(self):
            return self.closed

    loop = ClosedLoop()
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    registry._loop = loop
    invoked = []
    monkeypatch.setattr(registry, "_start_phase_retry", lambda *args: invoked.append(args))
    errors = []

    worker = None
    try:
        with ownership.state_session(layout):
            def enqueue_from_worker():
                try:
                    with ownership.state_session(layout):
                        registry._request_phase_retry(
                            "a" * 32, 1, "sig", {"leader_alive": True}
                        )
                except BaseException as exc:  # the bounded handoff reports the closed loop
                    errors.append(exc)

            worker = threading.Thread(target=enqueue_from_worker)
            worker.start()
            deadline = time.monotonic() + 1.0
            while not loop.callbacks and time.monotonic() < deadline:
                time.sleep(0.005)
            assert loop.callbacks, "worker never queued the phase callback"
            loop.closed = True
            worker.join(timeout=2.0)
            assert not worker.is_alive(), "closed-loop handoff was unbounded"
            assert not errors
            assert not invoked, "a callback queued after loop shutdown must not start a retry"
            del enqueue_from_worker
    finally:
        loop.closed = True
        if worker is not None:
            worker.join(timeout=2.0)

    # The queued callback was never executed, so only its temporary retained ref can
    # distinguish correct cleanup from a leaked ownership lease. The parent SH lease
    # must be gone before attempting EX; same-process SH-to-EX upgrades are refused.
    deadline = time.monotonic() + 2.0
    while True:
        try:
            with ownership.exclusive_session(layout, timeout=0.1):
                break
        except ownership.StateBusy:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)


def test_phase_retry_waits_for_slow_open_loop_then_runs_callback(tmp_path, monkeypatch):
    """A live loop may take longer than the handoff watchdog; its callback still runs."""
    root = tmp_path / "state"
    layout = _layout(root)

    class SlowLoop:
        def __init__(self):
            self.closed = False
            self.callbacks = []

        def call_soon_threadsafe(self, callback):
            self.callbacks.append(callback)
            return asyncio.Handle(callback, (), self)

        def get_debug(self):
            return False

        def is_closed(self):
            return self.closed

    loop = SlowLoop()
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    registry._loop = loop
    invoked = []
    def start_retry(*args):
        ownership.assert_owned(layout)
        invoked.append(args)
    monkeypatch.setattr(registry, "_start_phase_retry", start_retry)
    errors = []
    worker = None
    try:
        with ownership.state_session(layout):
            def enqueue_from_worker():
                try:
                    with ownership.state_session(layout):
                        registry._request_phase_retry("c" * 32, 1, "sig", {"leader_alive": True})
                except BaseException as exc:
                    errors.append(exc)

            worker = threading.Thread(target=enqueue_from_worker)
            worker.start()
            deadline = time.monotonic() + 1.0
            while not loop.callbacks and time.monotonic() < deadline:
                time.sleep(0.005)
            assert loop.callbacks
            time.sleep(1.1)
            worker.join(timeout=2.0)
            assert not worker.is_alive()
            assert not errors
        with pytest.raises(ownership.StateBusy):
            with ownership.exclusive_session(layout, timeout=0.1):
                pass
        loop.callbacks.pop(0)()
        assert invoked
        with ownership.exclusive_session(layout, timeout=1.0):
            pass
    finally:
        loop.closed = True
        if worker is not None:
            worker.join(timeout=2.0)


def test_registry_monitor_keeps_ownership_through_terminal_persist(tmp_path, monkeypatch):
    """A real monitor cannot release its job lease at ``Task.done`` before final persistence."""
    root = tmp_path / "state"
    layout = _layout(root)
    with ownership.state_session(layout):
        pass

    persist_entered = threading.Event()
    release_persist = threading.Event()
    final_write = root / "monitor-final-write"
    errors = []

    class Process:
        returncode = 0

        async def wait(self):
            return 0

    async def no_cancel_verdict(task, registry):
        return "none"

    monkeypatch.setattr(tasks, "_classify", lambda task, exit_code: "completed")
    monkeypatch.setattr(tasks, "_await_cancel_verdict", no_cancel_verdict)

    def run_monitor():
        async def runner():
            registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
            task = tasks.Task(
                task_id="b" * 32,
                backend="claude",
                session_id="session",
                repo_path=root,
                prompt="test",
                max_turns=1,
                log_path=layout.tasks_directory / ("b" * 32 + ".jsonl"),
                started_at=tasks._now(),
            )
            task.proc = Process()

            def persist(_task):
                persist_entered.set()
                if not release_persist.wait(2.0):
                    raise AssertionError("terminal persistence gate was not released")
                final_write.write_text("persisted")

            registry.persist = persist
            with ownership.state_session(layout):
                monitor = ownership.owned_job(layout, tasks._monitor(task, registry))
            await monitor
            assert task.done.is_set()

        try:
            asyncio.run(runner())
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_monitor)
    thread.start()
    try:
        assert persist_entered.wait(2.0), "monitor did not reach terminal persistence"
        assert not errors
        assert not final_write.exists()
        with pytest.raises(ownership.StateBusy):
            with ownership.exclusive_session(layout, timeout=0.1):
                pass
    finally:
        release_persist.set()
        thread.join(timeout=3.0)
    assert not thread.is_alive(), "monitor did not settle within the bounded test window"
    assert not errors
    assert final_write.read_text() == "persisted"
    with ownership.exclusive_session(layout, timeout=0.5):
        pass


@pytest.mark.asyncio
async def test_monitor_capture_ownership_error_sets_done_and_closes_eventlog(tmp_path, monkeypatch):
    """Capture rejection still settles the task and releases all monitor resources."""
    from harflow_mcp import task_outcomes

    root = tmp_path / "state"
    layout = _layout(root)

    class Process:
        returncode = 0

        async def wait(self):
            return 0

    async def no_cancel_verdict(task, registry):
        return "none"

    monkeypatch.setattr(tasks, "_classify", lambda task, exit_code: "completed")
    monkeypatch.setattr(tasks, "_await_cancel_verdict", no_cancel_verdict)

    def capture_failure(*args):
        raise ownership.StateIncompatible("capture ownership")

    monkeypatch.setattr(task_outcomes, "capture", capture_failure)
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    task_id = "h" * 32
    task = tasks.Task(task_id=task_id, backend="claude", session_id="session", repo_path=root,
                      prompt="test", max_turns=1,
                      log_path=layout.tasks_directory / f"{task_id}.jsonl", started_at=tasks._now())
    task.proc = Process()
    with ownership.state_session(layout):
        task.events = events.EventLog(events.events_path(layout.tasks_directory, task_id), task_id,
                                      layout=layout)
        monitor = ownership.owned_job(layout, tasks._monitor(task, registry), name="capture-failure")
    with pytest.raises(ownership.StateIncompatible):
        await monitor
    assert task.done.is_set()
    assert task.events._handle is None
    with ownership.exclusive_session(layout, timeout=1.0):
        pass


@pytest.mark.asyncio
async def test_owned_drainer_rejects_replaced_root_and_reaps_owned_child(tmp_path):
    """A queued owned drainer rejects a replaced root and reaps its owned child."""
    root = tmp_path / "state"
    layout = _layout(root)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(10)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task_id = "i" * 32
    task = tasks.Task(task_id=task_id, backend="claude", session_id="session", repo_path=root,
                      prompt="test", max_turns=1,
                      log_path=layout.tasks_directory / f"{task_id}.jsonl", started_at=tasks._now())
    task.proc, task.pgid = proc, proc.pid
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    try:
        with ownership.state_session(layout):
            job = ownership.owned_job(layout, tasks._drain_stdout(task, registry), name="queued-drainer")
        moved = tmp_path / "old-state"
        root.rename(moved)
        root.mkdir()
        with pytest.raises(ownership.StateIncompatible):
            await asyncio.wait_for(job, timeout=2)
        await asyncio.wait_for(proc.wait(), timeout=2)
        assert proc.returncode != 0
        assert task.drain_failed
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


@pytest.mark.asyncio
async def test_spawn_initial_persist_ownership_failure_reaps_real_child(tmp_path, monkeypatch):
    """A post-exec ownership failure during initial persist must not orphan the child."""
    from test_registry import _TrivialBackend
    from harflow_mcp.backends import Invocation

    class SlowBackend(_TrivialBackend):
        name = "slow-test"

        def build_start_argv(self, prompt, **kwargs):
            return Invocation([sys.executable, "-c", "import time; time.sleep(10)"])

    root = tmp_path / "state"
    layout = _layout(root)
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True, timeout=3)
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    captured = []
    real_create = tasks.asyncio.create_subprocess_exec

    async def capture_process(*args, **kwargs):
        proc = await real_create(*args, **kwargs)
        captured.append(proc)
        return proc

    monkeypatch.setattr(tasks.asyncio, "create_subprocess_exec", capture_process)

    def fail_persist(_task):
        raise ownership.StateIncompatible("initial persist ownership")

    registry.persist = fail_persist
    with pytest.raises(ownership.StateIncompatible):
        await registry.start("x", repo, backend=SlowBackend(), freedom="write_in_repo")
    assert captured
    proc = captured[0]
    try:
        await asyncio.wait_for(proc.wait(), timeout=2)
        assert proc.returncode != 0
        with ownership.exclusive_session(layout, timeout=1.0):
            pass
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


@pytest.mark.asyncio
async def test_abandoned_monitor_closes_eventlog_lease_before_root_reacquisition(tmp_path):
    """Cancelling a monitor must close its EventLog even when no terminal write is possible."""
    root = tmp_path / "state"
    layout = _layout(root)
    task_id = "c" * 32
    started = threading.Event()
    release = threading.Event()

    class Process:
        returncode = None

        async def wait(self):
            started.set()
            await asyncio.to_thread(release.wait, 2.0)
            return 0

    task = tasks.Task(
        task_id=task_id,
        backend="claude",
        session_id="session",
        repo_path=root,
        prompt="test",
        max_turns=1,
        log_path=layout.tasks_directory / f"{task_id}.jsonl",
        started_at=tasks._now(),
    )
    task.proc = Process()
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    event_log = None
    try:
        with ownership.state_session(layout):
            layout.tasks_directory.mkdir(parents=True, exist_ok=True)
            event_log = events.EventLog(events.events_path(layout.tasks_directory, task_id), task_id, layout=layout)
            task.events = event_log
            monitor = ownership.owned_job(layout, tasks._monitor(task, registry), name="abandoned-monitor")
        assert await asyncio.to_thread(started.wait, 1.0)
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)
        release.set()
        with ownership.exclusive_session(layout, timeout=0.1):
            pass
    finally:
        release.set()
        if event_log is not None:
            event_log.close()


@pytest.mark.asyncio
async def test_drainer_initial_ownership_failure_does_not_kill_inherited_child(tmp_path, monkeypatch):
    """A drainer running in a fork-inherited task must not signal the child process."""
    root = tmp_path / "state"
    layout = _layout(root)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(10)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task_id = "d" * 32
    task = tasks.Task(
        task_id=task_id, backend="claude", session_id="session", repo_path=root,
        prompt="test", max_turns=1, log_path=layout.tasks_directory / f"{task_id}.jsonl",
        started_at=tasks._now(),
    )
    task.proc = proc
    task.pgid = proc.pid
    task._process_owner_pid = os.getpid() + 100000
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)

    def reject(_layout):
        raise ownership.StateIncompatible("inherited task ownership")

    monkeypatch.setattr(tasks, "assert_owned", reject)
    try:
        with pytest.raises(ownership.StateIncompatible):
            await tasks._drain_stdout(task, registry)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=0.2)
    finally:
        if proc.returncode is None:
            proc.kill()
        await proc.wait()


@pytest.mark.asyncio
async def test_drainer_initial_ownership_failure_stops_owned_child(tmp_path, monkeypatch):
    """The same initial rejection for a locally owned task terminates and propagates."""
    root = tmp_path / "state"
    layout = _layout(root)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(10)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task_id = "e" * 32
    task = tasks.Task(
        task_id=task_id, backend="claude", session_id="session", repo_path=root,
        prompt="test", max_turns=1, log_path=layout.tasks_directory / f"{task_id}.jsonl",
        started_at=tasks._now(),
    )
    task.proc = proc
    task.pgid = proc.pid
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)

    def reject(_layout):
        raise ownership.StateIncompatible("owned task lost state ownership")

    monkeypatch.setattr(tasks, "assert_owned", reject)
    try:
        with pytest.raises(ownership.StateIncompatible):
            await asyncio.wait_for(tasks._drain_stdout(task, registry), timeout=2)
        await asyncio.wait_for(proc.wait(), timeout=2)
        assert proc.returncode is not None
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


@pytest.mark.asyncio
async def test_stdout_drainer_open_ownership_failure_stops_owned_child(tmp_path, monkeypatch):
    """Ownership failure while opening the raw stream kills the locally owned child."""
    root = tmp_path / "state"
    layout = _layout(root)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(10)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task_id = "f" * 32
    task = tasks.Task(task_id=task_id, backend="claude", session_id="session", repo_path=root,
                      prompt="test", max_turns=1,
                      log_path=layout.tasks_directory / f"{task_id}.jsonl", started_at=tasks._now())
    task.proc, task.pgid = proc, proc.pid
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    monkeypatch.setattr(tasks, "open_descriptor",
                        lambda *args, **kwargs: (_ for _ in ()).throw(
                            ownership.StateIncompatible("open ownership")))
    try:
        with pytest.raises(ownership.StateIncompatible):
            await asyncio.wait_for(tasks._drain_stdout(task, registry), timeout=2)
        await asyncio.wait_for(proc.wait(), timeout=2)
        assert proc.returncode is not None
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


@pytest.mark.asyncio
async def test_stdout_drainer_midstream_ownership_failure_closes_descriptor_and_stops_child(
    tmp_path, monkeypatch
):
    """A descriptor validation failure after a line is read closes resources and kills the child."""
    root = tmp_path / "state"
    layout = _layout(root)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import time; time.sleep(10)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task_id = "g" * 32
    task = tasks.Task(task_id=task_id, backend="claude", session_id="session", repo_path=root,
                      prompt="test", max_turns=1,
                      log_path=layout.tasks_directory / f"{task_id}.jsonl", started_at=tasks._now())
    task.proc, task.pgid = proc, proc.pid
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    reader = asyncio.StreamReader()
    reader.feed_data(b"not-json\\n")
    reader.feed_eof()
    task.proc = type("Proc", (), {"stdout": reader, "returncode": None, "pid": proc.pid})()
    descriptor = type("Descriptor", (), {})()
    descriptor.closed = False
    descriptor.lease = type("Lease", (), {
        "validate": lambda self: (_ for _ in ()).throw(
            ownership.StateIncompatible("midstream ownership")),
    })()
    descriptor.close = lambda: setattr(descriptor, "closed", True)
    handle = type("Handle", (), {})()
    handle.closed = False
    handle.write = lambda data: None
    handle.flush = lambda: None
    handle.tell = lambda: 1
    handle.close = lambda: setattr(handle, "closed", True)
    monkeypatch.setattr(tasks, "open_descriptor", lambda *args, **kwargs: descriptor)
    monkeypatch.setattr(tasks.os, "fdopen", lambda *args, **kwargs: handle)
    monkeypatch.setattr(tasks, "assert_owned", lambda _layout: None)
    try:
        with pytest.raises(ownership.StateIncompatible):
            await asyncio.wait_for(tasks._drain_stdout(task, registry), timeout=2)
        assert descriptor.closed
        assert handle.closed
        await asyncio.wait_for(proc.wait(), timeout=2)
        assert proc.returncode is not None
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()


@pytest.mark.asyncio
async def test_spawned_monitor_cancelled_before_first_step_still_closes_eventlog(tmp_path, monkeypatch):
    """The real spawn registration must close its EventLog when monitor startup is cancelled."""
    from test_registry import _TrivialBackend

    root = tmp_path / "state"
    layout = _layout(root)
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True, timeout=3)
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory, open_monitor=False)
    real_owned_job = tasks.owned_job

    def cancel_monitor(selected, coroutine, *, name=None):
        job = real_owned_job(selected, coroutine, name=name)
        if name and name.startswith("harflow-monitor-"):
            job.cancel()
        return job

    monkeypatch.setattr(tasks, "owned_job", cancel_monitor)
    task = await registry.start("x", repo, backend=_TrivialBackend(), freedom="write_in_repo")
    try:
        assert task.proc is not None
        await asyncio.wait_for(task.proc.wait(), timeout=3)
        await asyncio.wait_for(asyncio.gather(*task.watchers, return_exceptions=True), timeout=3)
        deadline = asyncio.get_running_loop().time() + 2
        while task.events is not None and task.events._handle is not None:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("pre-start monitor cancellation left EventLog open")
            await asyncio.sleep(0.01)
        with ownership.exclusive_session(layout, timeout=1.0):
            pass
    finally:
        if task.proc is not None and task.proc.returncode is None:
            task.proc.kill()
            await task.proc.wait()


class _async_session:
    def __init__(self, layout):
        self.layout = layout

    async def __aenter__(self):
        self.manager = ownership.state_session(self.layout)
        return self.manager.__enter__()

    async def __aexit__(self, *exc):
        return self.manager.__exit__(*exc)
