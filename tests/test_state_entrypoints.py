"""Process entrypoints keep lazy commands side-effect free and reject bad roots cleanly."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from harflow_mcp import detached, state_ownership, store, tasks
from harflow_mcp.state_layout import UnsupportedStateLayout, resolve_layout
from task_polling import read_task_until


def _run(root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["HARFLOW_STATE_ROOT"] = str(root)
    env.pop("HARFLOW_INTEGRATION", None)
    env.pop("PB_INTEGRATION", None)
    env.pop("HARFLOW_CLI_INTEGRATION", None)
    env.pop("PB_CLI_INTEGRATION", None)
    env["PYTHONPATH"] = "src"
    return subprocess.run(
        [sys.executable, "-m", "harflow_mcp.cli", *arguments],
        cwd=Path(__file__).parents[1],
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )


def test_help_and_version_info_do_not_create_state(tmp_path: Path) -> None:
    root = tmp_path / "state"
    help_result = _run(root, "--help")
    version_result = _run(root, "version-info", "--json")
    assert help_result.returncode == 0
    assert version_result.returncode == 0
    assert json.loads(version_result.stdout)["product"] == "harflow"
    assert not root.exists()
    assert help_result.stderr == version_result.stderr == ""


def test_invalid_marker_json_cli_error_is_clean_and_does_not_create_lock(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / ".harflow-store.json").write_text('{"product":"foreign","v":1}\n')
    result = _run(root, "list", "--json")
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "state_incompatible"
    assert "Traceback" not in result.stderr
    assert not (root / ".harflow-store.lock").exists()


def test_nonempty_unmarked_root_stdio_cli_reports_structured_incompatibility(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.txt").write_text("foreign")
    result = _run(root, "list", "--json")
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "state_incompatible"
    assert "Traceback" not in result.stderr
    assert not (root / ".harflow-store.lock").exists()


def test_deeply_nested_marker_is_clean_incompatible_error(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    nested = "[" * 7800 + "]" * 7800
    (root / ".harflow-store.json").write_text(
        '{"product":"harflow","format":"harflow.store","v":1,'
        '"product_version":"0.0.0","layout_version":1,"nested":' + nested + "}\n"
    )
    result = _run(root, "list", "--json")
    assert result.returncode == 1
    assert json.loads(result.stdout)["error"]["code"] == "state_incompatible"
    assert "Traceback" not in result.stderr
    assert not (root / ".harflow-store.lock").exists()


def test_busy_root_json_cli_is_clean_and_has_no_stdout_on_stdio_mode(tmp_path: Path) -> None:
    root = tmp_path / "state"
    layout = resolve_layout(root=root)
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
        record = store.TaskRecord(
            task_id="d" * 32, backend="claude", session_id="session", repo_path=str(tmp_path),
            started_at="now", status="running",
        )
        store.write(layout.tasks_directory, record)
    holder_code = """
import sys
from pathlib import Path
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import exclusive_session
with exclusive_session(resolve_layout(root=Path(sys.argv[1])), timeout=.5):
 print('ready', flush=True)
 sys.stdin.read(1)
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_code, str(root)], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )
    try:
        assert holder.stdout.readline().strip() == "ready"
        result = _run(root, "status", record.task_id, "--json")
        assert result.returncode == 1
        assert json.loads(result.stdout)["error"]["code"] == "state_busy"
        assert "Traceback" not in result.stderr
        plain = _run(root, "status", record.task_id)
        assert plain.returncode == 1
        assert plain.stdout == ""
        assert "Traceback" not in plain.stderr
    finally:
        if holder.stdin:
            holder.stdin.write("x")
            holder.stdin.flush()
        try:
            holder.wait(timeout=2)
        except subprocess.TimeoutExpired:
            holder.kill()
            holder.wait(timeout=2)


def test_busy_root_stdio_server_refuses_before_emitting_mcp_bytes(tmp_path: Path) -> None:
    """The real stdio server reports an external EX holder without corrupting stdout."""
    root = tmp_path / "state"
    layout = resolve_layout(root=root)
    with state_ownership.state_session(layout):
        pass
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    env["HARFLOW_STATE_ROOT"] = str(root)
    holder_code = """
from pathlib import Path
import sys
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import exclusive_session
with exclusive_session(resolve_layout(root=Path(sys.argv[1])), timeout=.5):
    print('ready', flush=True)
    sys.stdin.read(1)
"""
    holder = subprocess.Popen(
        [sys.executable, "-c", holder_code, str(root)], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )
    server = None
    try:
        assert holder.stdout.readline().strip() == "ready"
        server = subprocess.Popen(
            [sys.executable, "-m", "harflow_mcp.server"], cwd=Path(__file__).parents[1],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env,
        )
        out, err = server.communicate(timeout=10)
        assert server.returncode == 2
        assert out == ""
        assert "Traceback" not in err
    finally:
        if server is not None and server.poll() is None:
            server.kill()
            server.communicate(timeout=2)
        if holder.stdin:
            holder.stdin.write("x")
            holder.stdin.flush()
        try:
            holder.wait(timeout=2)
        except subprocess.TimeoutExpired:
            holder.kill()
            holder.wait(timeout=2)


def test_invalid_root_plain_cli_error_has_no_stdout_or_traceback(tmp_path: Path) -> None:
    root = tmp_path / "state"
    root.mkdir()
    (root / "foreign.txt").write_text("foreign")
    result = _run(root, "list")
    assert result.returncode == 1
    assert result.stdout == ""
    assert "Traceback" not in result.stderr


def test_detached_child_retains_root_ownership_through_final_task_writes(tmp_path: Path) -> None:
    root = tmp_path / "home"
    root.mkdir()
    selected = resolve_layout(root=root / ".harflow")
    with state_ownership.state_session(selected):
        pass
    selected.tasks_directory.mkdir(parents=True, exist_ok=True)
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], timeout=3, check=True)
    env = os.environ.copy()
    env.update(HOME=str(root), HARFLOW_FAKE_SCRIPT="sleep 2", PYTHONPATH="src")
    env.pop("HARFLOW_STATE_ROOT", None)
    env.pop("HARFLOW_INTEGRATION", None)
    env.pop("PB_INTEGRATION", None)
    child = subprocess.run(
        [sys.executable, "tests/ctl_driver.py", "ctl", "run", "--backend", "fake", "--repo", str(repo), "--prompt", "hi", "--json"],
        cwd=Path(__file__).parents[1], env=env, capture_output=True, text=True, timeout=5, check=False,
    )
    assert child.returncode == 0, child.stderr
    document = json.loads(child.stdout)
    task_id = document["result"]["task_id"]
    tasks_directory = root / ".harflow" / "tasks"
    layout = selected
    deadline = __import__("time").monotonic() + 8
    record = read_task_until(tasks_directory, task_id, lambda record: True, deadline=deadline)
    assert record is not None
    with pytest.raises(state_ownership.StateBusy):
        with state_ownership.exclusive_session(layout, timeout=0.2):
            pass
    record = read_task_until(
        tasks_directory,
        task_id,
        lambda record: record is not None and record.status in store.TERMINAL_RECORD_STATUSES,
        deadline=deadline,
    )
    assert record is not None and record.status == "completed"
    with state_ownership.exclusive_session(layout, timeout=1.0):
        pass


def test_detached_invalid_root_refuses_before_ctl_log_or_task_directory_creation(tmp_path: Path) -> None:
    root = tmp_path / "foreign"
    root.mkdir()
    (root / "foreign.txt").write_text("foreign")
    directory = root / "streams"

    async def action(registry):
        raise AssertionError("detached action must not start")

    with pytest.raises(UnsupportedStateLayout):
        detached.run_detached(
            action,
            log_path=root / "ctl.log",
            log_dir=directory,
            registry_factory=lambda: tasks.TaskRegistry(log_dir=directory, open_monitor=False),
            timeout=1.0,
        )
    assert not (root / "ctl.log").exists()
    assert not directory.exists()
    assert not (root / ".harflow-store.lock").exists()


@pytest.mark.parametrize("kind", ["foreign_marker", "nonempty_unmarked"])
def test_detached_canonical_root_rejects_state_before_ctl_log_or_task_writes(tmp_path: Path, kind: str) -> None:
    root = tmp_path / "state"
    directory = root / "tasks"
    root.mkdir()
    directory.mkdir()
    if kind == "foreign_marker":
        (root / ".harflow-store.json").write_text('{"product":"foreign","v":1}\n')
    else:
        (root / "foreign.txt").write_text("foreign")

    async def action(registry):
        raise AssertionError("detached action must not start")

    outcome = detached.run_detached(
        action,
        log_path=root / "ctl.log",
        log_dir=directory,
        registry_factory=lambda: tasks.TaskRegistry(log_dir=directory, open_monitor=False),
        timeout=2.0,
    )
    assert outcome.kind == "error"
    assert outcome.payload["code"] == "state_incompatible"
    assert not (root / "ctl.log").exists()
    assert not (root / ".harflow-store.lock").exists()


def test_supervisor_main_waits_for_cancelled_executor_thread_before_process_exit(tmp_path: Path) -> None:
    root = tmp_path / "state"
    control = tmp_path / "control"
    control.mkdir()
    with state_ownership.state_session(resolve_layout(root=root)):
        pass
    (root / "tasks").mkdir(parents=True, exist_ok=True)
    script = r'''
import asyncio, sys
from pathlib import Path
from harflow_mcp import state_ownership, workflows
from harflow_mcp.state_layout import resolve_layout

root, control = Path(sys.argv[1]), Path(sys.argv[2])
layout = resolve_layout(root=root)
release = control / "release"
ready = control / "ready"

def late_write():
    ready.write_text("thread")
    while not release.exists():
        import time
        time.sleep(0.01)
    (root / "late-state-write").write_text("done")

async def fake_main(args):
    worker = asyncio.create_task(asyncio.to_thread(late_write))
    while not ready.exists():
        await asyncio.sleep(0.01)
    worker.cancel()
    try:
        await worker
    except asyncio.CancelledError:
        pass
    (control / "main-returned").write_text("returned")

workflows._main = fake_main
workflows.main(["supervise", "run", "--root", str(root)])
'''
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(root), str(control)],
        cwd=Path(__file__).parents[1],
        env={**os.environ, "PYTHONPATH": "src"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        import time
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline and not (control / "main-returned").exists():
            time.sleep(0.01)
        assert (control / "main-returned").exists()
        with pytest.raises(state_ownership.StateBusy):
            with state_ownership.exclusive_session(resolve_layout(root=root), timeout=0.2):
                pass
        (control / "release").write_text("go")
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, stderr
        assert (root / "late-state-write").read_text() == "done"
        with state_ownership.exclusive_session(resolve_layout(root=root), timeout=1.0):
            pass
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=2)
