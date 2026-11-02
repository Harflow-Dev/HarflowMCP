"""No-paid Vibe TaskRegistry qualification: real pump, approval ledger and settlement."""
from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import sys
import time
import tomllib

import pytest

from harflow_mcp import approvals, identity, state_ownership
from harflow_mcp.backends import get
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry
from test_vibe_commands_cli import provider

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    os.environ.get("HARFLOW_CLI_INTEGRATION") != "1" or sys.platform != "darwin",
    reason="requires qualified macOS Vibe 3.0.3 and localhost listener")]


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    binary = shutil.which("vibe")
    if not binary or not shutil.which("vibe-acp"):
        pytest.skip("Vibe ACP not installed")
    if subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=10).stdout.strip() != "vibe 3.0.3":
        pytest.skip("qualified against Vibe 3.0.3")
    # Isolate every provider/config environment source; do not inherit credentials.
    for key in tuple(os.environ):
        if key not in ("PATH", "LANG", "SYSTEMROOT", "TMPDIR"):
            monkeypatch.delenv(key, raising=False)
    for name in ("home", "vibe", "repo", "tmp"):
        (tmp_path / name).mkdir()
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("VIBE_HOME", str(tmp_path / "vibe"))
    monkeypatch.setenv("TMPDIR", str(tmp_path / "tmp"))
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    monkeypatch.setenv("MISTRAL_API_KEY", "local-only-placeholder")
    monkeypatch.setenv("HARFLOW_LOCAL_TEST_KEY", "local-only-placeholder")
    (tmp_path / "repo" / "Makefile").write_text("test:\n\ttouch marker\nother:\n\ttouch other-marker\n")
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        registry = TaskRegistry(log_dir=root / "tasks", owner=identity.own_identity())
        yield tmp_path, registry


def configure(base, url):
    (base / "vibe" / "config.toml").write_text(f'''active_model="unselected"
enable_connectors=false
enable_telemetry=false
api_retry_max_elapsed_time=0.1
[[providers]]
name="local"
api_base="{url}"
api_key_env_var="HARFLOW_LOCAL_TEST_KEY"
[[models]]
name="local"
alias="local"
provider="local"
thinking="off"
[[models]]
name="unselected"
alias="unselected"
provider="local"
thinking="off"
''')


async def pending(registry, task):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        rows = [row for row in approvals.rows(registry.log_dir, task.task_id)
                if row["disposition"] == "pending" and row["delivery"] == "pending"]
        if rows:
            return rows[0]
        if task.done.is_set():
            raise AssertionError(f"task settled without approval: {task.snapshot()} stderr={task.stderr_tail}")
        await asyncio.sleep(.05)
    raise AssertionError(f"no approval: {task.snapshot()} stderr={task.stderr_tail}")


def decide(registry, task, row, decision):
    approvals.reserve(registry.log_dir, task.task_id, row["request_id"], row["action_digest"],
                      row["owner_revision"], decision, row["generation"])
    task.pump_wake.set()


async def settled(task):
    await asyncio.wait_for(task.done.wait(), 45)
    assert task.status == "completed", (task.snapshot(), task.stderr_tail)
    assert task.session_id
    assert task.snapshot()["exit_code"] == 0
    assert task.acc.summary == "LOCAL ANSWER"
    assert task.acc.terminal["stop_reason"] == "end_turn"
    snapshot = task.snapshot()
    assert snapshot["output_drained"] is True
    assert snapshot["model"] == "local"
    usage = snapshot["usage"]
    assert usage["input_tokens"] >= 20 and usage["output_tokens"] >= 20
    assert usage["total_tokens"] == usage["input_tokens"] + usage["output_tokens"]
    # The measured ACP result reports tokens, but no USD price or turn count.
    assert snapshot["total_cost_usd"] is None
    assert snapshot["num_turns"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["approve", "reject"])
async def test_vibe_registry_caller_ledger_round_trip(isolated, decision):
    base, registry = isolated
    with provider("runtime", ("bash", {"command": "make test"})) as (url, requests):
        configure(base, url)
        task = await registry.start("registry caller measurement", base / "repo", backend=get("vibe"),
                                    caller_approvals=True, model="local")
        row = await pending(registry, task)
        assert row["action"]["tool_name"] == "bash"
        assert row["action"]["input"]["command"] == "make test"
        assert not (base / "repo" / "marker").exists()
        decide(registry, task, row, decision)
        await settled(task)
    assert (base / "repo" / "marker").exists() == (decision == "approve")
    final, = approvals.rows(registry.log_dir, task.task_id)
    assert final["delivered_decision"] == decision
    assert final["execution_outcome"] == ("succeeded" if decision == "approve" else "failed")
    assert any(request.get("model") == "local" for request in requests)
    assert not any(request.get("model") == "unselected" for request in requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", [False, True])
async def test_vibe_registry_grants_and_explicit_resume(isolated, caller):
    base, registry = isolated
    with provider("runtime", ("bash", {"command": "make test"})) as (url, requests):
        configure(base, url)
        first = await registry.start("registry grant measurement", base / "repo", backend=get("vibe"),
                                     allowed_commands=["make test"], caller_approvals=caller, model="local")
        await settled(first)
        assert (base / "repo" / "marker").exists()
        assert approvals.rows(registry.log_dir, first.task_id) == []
        (base / "repo" / "marker").unlink()
        second = await registry.resume(first, "resumed registry grant measurement")
        await settled(second)
        assert second.proc.pid != first.proc.pid
        assert second.session_id == first.session_id
        assert (base / "repo" / "marker").exists()
        assert approvals.rows(registry.log_dir, second.task_id) == []
    assert any(request.get("model") == "local" for request in requests)
    assert not any(request.get("model") == "unselected" for request in requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", [False, True])
async def test_vibe_registry_unmatched_grant_uses_normal_approval_path(isolated, caller):
    base, registry = isolated
    with provider("runtime", ("bash", {"command": "make test"})) as (url, requests):
        configure(base, url)
        task = await registry.start("registry unmatched grant measurement", base / "repo", backend=get("vibe"),
                                    allowed_commands=["make other"], caller_approvals=caller, model="local")
        if caller:
            row = await pending(registry, task)
            assert row["action"]["input"]["command"] == "make test"
            decide(registry, task, row, "reject")
        await settled(task)
    assert not (base / "repo" / "marker").exists()
    rows = approvals.rows(registry.log_dir, task.task_id)
    if caller:
        final, = rows
        assert final["delivered_decision"] == "reject"
        assert final["execution_outcome"] == "failed"
    else:
        assert rows == []
    assert any(request.get("model") == "local" for request in requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["approve", "reject"])
async def test_vibe_registry_sensitive_read_approval(isolated, decision):
    base, registry = isolated
    target = base / "repo" / ".env"
    target.write_text("SYNTHETIC_FIXTURE_VALUE=local-only\n")
    with provider("runtime", ("read_file", {"path": str(target)})) as (url, requests):
        configure(base, url)
        task = await registry.start("sensitive read measurement", base / "repo", backend=get("vibe"),
                                    freedom="read_only", caller_approvals=True, model="local")
        row = await pending(registry, task)
        assert row["action"]["tool_name"] == "read_file"
        assert row["action"]["input"]["filePath"] == str(target)
        decide(registry, task, row, decision)
        await settled(task)
    final, = approvals.rows(registry.log_dir, task.task_id)
    assert final["delivered_decision"] == decision
    assert final["execution_outcome"] == ("succeeded" if decision == "approve" else "failed")
    tool_results = [message for request in requests for message in request.get("messages", [])
                    if message.get("role") == "tool"]
    assert bool(tool_results)
    assert any("SYNTHETIC_FIXTURE_VALUE" in str(message.get("content")) for message in tool_results) == (decision == "approve")
    assert target.read_text() == "SYNTHETIC_FIXTURE_VALUE=local-only\n"


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["approve", "reject"])
async def test_vibe_registry_outside_file_write_approval(isolated, decision):
    base, registry = isolated
    target = base / "outside-file"
    content = "Exact synthetic file content.\nSecond line.\n"
    with provider("runtime", ("write_file", {"path": str(target), "content": content})) as (url, requests):
        configure(base, url)
        task = await registry.start("outside file write measurement", base / "repo", backend=get("vibe"),
                                    freedom="read_only", caller_approvals=True, caller_write_approvals=True, model="local")
        row = await pending(registry, task)
        assert row["action"]["tool_name"] == "write_file"
        assert row["action"]["input"] == {"filePath": str(target), "content": content}
        assert row["inside_repository"] is False
        assert not target.exists()
        decide(registry, task, row, decision)
        await settled(task)
    assert target.exists() == (decision == "approve")
    if decision == "approve":
        assert target.read_text() == content
    final, = approvals.rows(registry.log_dir, task.task_id)
    assert final["delivered_decision"] == decision
    assert final["execution_outcome"] == ("succeeded" if decision == "approve" else "failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("protected", [False, True])
async def test_vibe_registry_file_write_system_refusals(isolated, protected):
    base, registry = isolated
    target = base / "vibe" / "config.toml" if protected else base / "outside-file"
    with provider("runtime", ("write_file", {"path": str(target), "content": "must not land"})) as (url, requests):
        configure(base, url)
        before = target.read_bytes() if target.exists() else None
        task = await registry.start("file write system refusal measurement", base / "repo", backend=get("vibe"),
                                    freedom="read_only", caller_approvals=True,
                                    caller_write_approvals=protected, model="local")
        await settled(task)
    if before is None:
        assert not target.exists()
    else:
        # The attack would overwrite the entire file. Check it independently
        # from Vibe's own model-selection persistence, covered separately.
        assert b"must not land" not in target.read_bytes()
        assert tomllib.loads(target.read_text())["enable_connectors"] is False
        assert tomllib.loads(target.read_text())["providers"] == tomllib.loads(before.decode())["providers"]
    final, = approvals.rows(registry.log_dir, task.task_id)
    assert final["system_rejection"] == ("protected_path" if protected else "write_approval_disabled")
    assert final["disposition"] == "system_rejected"
    assert final["effective_decision"] == "reject"
    assert final["execution_outcome"] == "failed"


@pytest.mark.asyncio
async def test_vibe_registry_task_model_does_not_mutate_user_model_configuration(isolated):
    base, registry = isolated
    with provider("runtime", ("bash", {"command": "make test"})) as (url, requests):
        configure(base, url)
        config_path = base / "vibe" / "config.toml"
        before = tomllib.loads(config_path.read_text())
        assert before["active_model"] == "unselected"
        task = await registry.start("task-scoped model measurement", base / "repo", backend=get("vibe"),
                                    freedom="read_only", model="local")
        await settled(task)
        assert tomllib.loads(config_path.read_text()) == before
    assert any(request.get("model") == "local" for request in requests)


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["approve", "reject"])
async def test_vibe_registry_exact_edit_approval(isolated, decision):
    base, registry = isolated
    target = base / "outside-edit.txt"
    original = "before\nReplace exactly this line.\nafter\n"
    old = "Replace exactly this line."
    new = "Approved replacement."
    target.write_text(original)
    tool_input = {"file_path": str(target), "old_string": old, "new_string": new}
    with provider("runtime", ("edit", tool_input)) as (url, requests):
        configure(base, url)
        task = await registry.start("exact edit measurement", base / "repo", backend=get("vibe"),
                                    freedom="read_only", caller_approvals=True, caller_write_approvals=True,
                                    model="local")
        row = await pending(registry, task)
        declared = [tool["function"]["parameters"] for request in requests for tool in request.get("tools", [])
                    if tool.get("function", {}).get("name") == "edit"]
        assert declared
        assert {"file_path", "old_string", "new_string"} <= set(declared[0]["required"])
        assert row["action"]["tool_name"] == "edit"
        raw = row["action"]["input"]
        assert raw["filePath"] == str(target)
        assert raw["oldString"] == old and raw["newString"] == new
        assert raw.get("replaceAll", False) is False
        assert row["inside_repository"] is False
        assert target.read_text() == original
        decide(registry, task, row, decision)
        await settled(task)
    assert target.read_text() == (original.replace(old, new) if decision == "approve" else original)
    final, = approvals.rows(registry.log_dir, task.task_id)
    assert final["delivered_decision"] == decision
    assert final["execution_outcome"] == ("succeeded" if decision == "approve" else "failed")


@pytest.mark.asyncio
async def test_vibe_registry_project_policy_reload_exposes_only_final_session(isolated):
    base, registry = isolated
    repo = base / 'repo'
    subprocess.run(['git', 'init', '-q', str(repo)], check=True, capture_output=True)
    (repo / '.vibe').mkdir()
    (repo / '.vibe' / 'config.toml').write_text('[tools.bash]\npermission="never"\n')
    with provider('runtime', ('bash', {'command': 'make test'})) as (url, requests):
        configure(base, url)
        task = await registry.start('project policy measurement', repo, backend=get('vibe'),
                                    caller_approvals=True, model='local')
        await settled(task)
    assert not (repo / 'marker').exists()
    assert approvals.rows(registry.log_dir, task.task_id) == []
    assert task.session_id == task.acc.session_id
    assert not any('instead of the requested' in notice for notice in task.bridge_notices)
    assert task.snapshot()['denial_categories'] == ['policy_denied']
