"""Generic approval seams: backend-provided frame routing, paths, protected targets, transport text and the control pipe.

Claude's values are pinned to what the generic code hard-coded before the seams existed; every other
backend gets the fail-closed defaults of the base mixin.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_paths, approval_runtime, approvals, backends, store
from harflow_mcp.backends import STDIN_DEVNULL, Invocation, get
from harflow_mcp.backends.base import Enforcement, ExtraPathsUnsupported, UnsupportedCapability
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.tasks import TaskRegistry
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout
from test_caller_write_approvals import CONTEXT, Q310, Registry, event, repo, runtime, runtime_task, task_stub, tmp_path  # noqa: F401

OWNER = {"pid": 1, "start_time": "t", "markers": []}


# --- Claude's values ------------------------------------------------------------------------------

def test_claude_frame_kind_and_request_key():
    backend = get("claude")
    assert backend.approval_frame_kind(event("Bash", {"command": "x"})) == "request"
    assert backend.approval_frame_kind({"type": "control_cancel_request", "request_id": "native-1"}) == "cancellation"
    assert backend.approval_frame_kind({"type": "control_cancel_request", "request_id": 3}) is None
    for other in ({"type": "assistant"}, {"type": "control_response"}, {}):
        assert backend.approval_frame_kind(other) is None
    assert backend.approval_request_key(event("Bash", {"command": "x"}, 7)) == "native-7"
    assert backend.approval_request_key({"type": "control_cancel_request", "request_id": "native-7"}) == "native-7"
    assert backend.approval_request_key({"type": "control_request", "request_id": 7}) is None


def test_claude_paths_write_tools_and_protected_config():
    backend = get("claude")
    for tool in ("Read", "Write", "Edit"):
        assert backend.approval_paths({"action": {"tool_name": tool, "input": {"file_path": "/a/b"}}}) == ["/a/b"]
    assert backend.approval_paths({"action": {"tool_name": "Bash", "input": {"command": "x"}}}) == []
    # An oversize audit row carries the path beside an empty input.
    assert backend.approval_paths({"action": {"tool_name": "Write", "input": {}}, "file_path": "/big"}) == ["/big"]
    assert backend.approval_paths({"action": {"tool_name": "Write", "input": {"file_path": 3}}}) == []
    assert backend.approval_write_tools() == frozenset({"Write", "Edit"})
    assert backend.protected_config_paths(Path("/repo")) == []


def test_claude_preview_transport_and_limitations_text_is_unchanged():
    backend = get("claude")
    assert backend.approval_transport() == "stdio_control"
    assert backend.approval_limitations(False) == ("Exact original action only (Bash, Read on qualified versions); saved harness rules remain effective. "
                                                   "Approving a Read can disclose files outside the repository. Claude has no bridge OS sandbox.")
    assert backend.approval_limitations(True) == ("Exact original action only (Bash, Read, Write and Edit on qualified versions); saved harness rules remain effective. "
                                                  "Approving a Read can disclose files outside the repository. Claude has no bridge OS sandbox.")


# --- fail-closed defaults -------------------------------------------------------------------------

@pytest.mark.parametrize("name", ["opencode", "vibe", "antigravity"])
def test_backends_without_the_seams_default_closed(name):
    backend = get(name)
    assert backend.approval_frame_kind({"type": "control_request", "request_id": "x"}) is None
    assert backend.approval_request_key({"type": "control_request", "request_id": "x"}) is None
    # No safe default for paths or write tools: a backend that routes requests must declare both.
    with pytest.raises(UnsupportedCapability):
        backend.approval_paths({"action": {"tool_name": "Write", "input": {"file_path": "/a"}}})
    with pytest.raises(UnsupportedCapability):
        backend.approval_write_tools()
    assert backend.protected_config_paths(Path("/repo")) == []
    assert backend.approval_transport() is None
    assert "Claude" not in backend.approval_limitations(True)
    assert backend.control_pipe_finished(backends.Accumulator()) is False
    assert backend.outgoing_frames("initial", None) == [] and backend.outgoing_frames("turn", backends.Accumulator()) == []


def test_control_pipe_is_refused_by_a_backend_without_one():
    invocation = Invocation(["x"], control_pipe=True)
    assert "control_pipe" in backends.base.classic_invocation_problem(invocation)
    assert backends.base.classic_invocation_problem(Invocation(["x"])) is None


def test_control_pipe_is_only_valid_on_a_devnull_invocation_without_initial_input():
    Invocation(["x"], control_pipe=True)
    for mode in ("pipe", "pipe_once"):
        with pytest.raises(ValueError, match="control_pipe"):
            Invocation(["x"], stdin_mode=mode, control_pipe=True)
    with pytest.raises(ValueError, match="control_pipe"):
        Invocation(["x"], initial_input=b"x", control_pipe=True)


def test_claude_assert_safe_rejects_the_control_pipe_flag():
    from harflow_mcp.backends.claude import UnsafeInvocationError
    with pytest.raises(UnsafeInvocationError, match="control pipe"):
        get("claude").assert_safe(Invocation(["claude"], control_pipe=True), "write_in_repo")


@pytest.mark.asyncio
async def test_a_request_routing_backend_that_declares_no_paths_is_refused_never_admitted(runtime):
    directory, registry = runtime

    class FrameOnly(ExtraPathsUnsupported):
        name = "frame-only"

        def approval_frame_kind(self, event):
            return "request" if event.get("type") == "ask" else None

        def approval_request_key(self, event):
            return "s:" + event["id"]

        def decode_approval_request(self, event, qualification=None):
            return {"native_request_id": "s:" + event["id"], "tool_use_id": event["id"], "action": {"tool_name": "cmd", "input": {"command": "x"}}}

    task = runtime_task(directory)
    task.caller_approvals = True
    assert await approval_runtime.observe(registry, task, FrameOnly(), {"type": "ask", "id": "r1"}) is True
    (row,) = approvals.rows(directory, "task-1")
    assert row["disposition"] == "unsupported" and row["delivery"] == "failed" and row["tool_use_id"] == "unsupported"
    assert registry.stopped == 1 and "Unsupported" in task.bridge_notices[-1]


@pytest.mark.asyncio
async def test_protected_targets_still_apply_for_a_backend_that_declares_its_paths(runtime, tmp_path_factory):
    directory, registry = runtime
    base = tmp_path_factory.mktemp("declared")
    work = base / "work"
    work.mkdir()
    subprocess.run(["git", "init", "-q", str(work)], check=True)
    secret = base / "backend-config"
    secret.mkdir()

    class Declared(PatchBackend):
        name = "declared"

        def approval_frame_kind(self, event):
            return "request" if event.get("type") == "ask" else None

        def approval_request_key(self, event):
            return "s:" + event["id"]

        def decode_approval_request(self, event, qualification=None):
            return {"native_request_id": "s:" + event["id"], "tool_use_id": event["id"], "action": {"tool_name": "patch", "input": {"paths": [str(secret / "config.toml")]}}}

    task = runtime_task(directory)
    task.caller_approvals, task.repo_path = True, work
    assert await approval_runtime.observe(registry, task, Declared(secret), {"type": "ask", "id": "r1"}) is True
    (row,) = approvals.rows(directory, "task-1")
    assert row["system_rejection"] == "protected_path"


# --- generic runtime uses the backend's frames ----------------------------------------------------

class FramedClaude:
    """Claude's decoder behind a different wire: requests are `{"kind": "ask", "id": ..., "claude": <claude event>}`."""

    def __init__(self):
        self.inner = ClaudeBackend()

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def approval_frame_kind(self, event):
        return {"ask": "request", "withdraw": "cancellation", "done": "resolved"}.get(event.get("kind"))

    def approval_request_key(self, event):
        return "i:" + str(event["id"]) if "id" in event else None

    def decode_approval_request(self, event, qualification=None):
        decoded = self.inner.decode_approval_request(event["claude"], qualification)
        decoded["native_request_id"] = self.approval_request_key(event)
        return decoded


def framed(index=1, tool="Bash", tool_input=None):
    return {"kind": "ask", "id": index, "claude": event(tool, tool_input or {"command": "ok"}, index)}


@pytest.mark.asyncio
async def test_observe_routes_requests_cancellations_and_resolutions_through_the_backend(runtime):
    directory, registry = runtime
    backend = FramedClaude()
    task = runtime_task(directory)
    task.caller_approvals = True
    assert await approval_runtime.observe(registry, task, backend, framed(1)) is True
    # A Claude-shaped control_request is no longer a request for this backend.
    assert await approval_runtime.observe(registry, task, backend, event("Bash", {"command": "other"}, 2)) is False
    (row,) = approvals.rows(directory, "task-1")
    assert row["native_request_id"] == "i:1" and row["disposition"] == "pending"
    await approval_runtime.observe(registry, task, backend, {"kind": "withdraw", "id": 1})
    assert approvals.rows(directory, "task-1")[0]["disposition"] == "invalidated"
    assert await approval_runtime.observe(registry, task, backend, framed(2)) is True
    await approval_runtime.observe(registry, task, backend, {"kind": "done", "id": 2})
    second = [r for r in approvals.rows(directory, "task-1") if r["native_request_id"] == "i:2"][0]
    assert second["disposition"] == "invalidated"


@pytest.mark.asyncio
async def test_the_drainer_gate_is_the_backend_frame_kind():
    import inspect
    from harflow_mcp import tasks
    source = inspect.getsource(tasks)
    assert '"control_request"' not in source
    assert '"control_request"' not in inspect.getsource(approval_runtime)


# --- path facts and protected targets --------------------------------------------------------------

class PatchBackend:
    """Two paths per action, a backend-named write tool and a backend-protected location."""

    def __init__(self, protected):
        self.protected = protected

    def approval_paths(self, decoded):
        return list(decoded["action"]["input"]["paths"])

    def approval_write_tools(self):
        return frozenset({"patch"})

    def protected_config_paths(self, repo):
        return [self.protected]


def patch_action(*paths, tool="patch"):
    return {"native_request_id": "n", "tool_use_id": "t", "action": {"tool_name": tool, "input": {"paths": list(paths)}}}


def test_multi_path_actions_record_each_containment_fact_and_refuse_a_protected_member(repo):
    secret = repo.base / "backend-config"
    secret.mkdir()
    task = SimpleNamespace(repo_path=repo.repo)
    registry = SimpleNamespace(log_dir=repo.log_dir)
    backend = PatchBackend(secret)
    inside, outside = str(repo.repo / "a.txt"), str(repo.base / "outside.txt")
    decoded = approval_runtime._path_facts(registry, task, patch_action(inside, outside), backend)
    assert decoded["path_facts"] == [{"file_path": inside, "inside_repository": True}, {"file_path": outside, "inside_repository": False}]
    assert decoded["inside_repository"] is False and "system_rejection" not in decoded
    refused = approval_runtime._path_facts(registry, task, patch_action(inside, str(secret / "config.toml")), backend)
    assert refused["system_rejection"] == "protected_path"
    # A non-write tool is checked against the state root only.
    read_only = approval_runtime._path_facts(registry, task, patch_action(str(secret / "config.toml"), tool="view"), backend)
    assert "system_rejection" not in read_only
    one = approval_runtime._path_facts(registry, task, patch_action(inside), backend)
    assert "path_facts" not in one and one["inside_repository"] is True
    none = approval_runtime._path_facts(registry, task, patch_action(), backend)
    assert "inside_repository" not in none


def test_protected_targets_default_has_no_backend_group_and_claude_adds_nothing(repo):
    targets = approval_paths.protected_targets(repo.repo, repo.log_dir, extra=get("claude").protected_config_paths(repo.repo))
    assert targets["backend"] == []
    guarded = approval_paths.protected_targets(repo.repo, repo.log_dir, extra=[repo.base / "extra"])
    assert approval_paths.protection(guarded, "patch", str(repo.base / "extra" / "x"), repo.repo, frozenset({"patch"})) == "protected_path"
    assert approval_paths.protection(guarded, "patch", str(repo.base / "extra" / "x"), repo.repo) is None


def test_approved_writes_disclose_one_path_unchanged_and_several_additively(repo):
    from test_caller_write_approvals import admit
    directory = repo.log_dir
    single = admit(directory, {**get("claude").decode_approval_request(event("Write", {"file_path": "/out/a.txt", "content": "c"}), Q310), "inside_repository": False})
    multi = admit(directory, {"native_request_id": "n2", "tool_use_id": "t2", "action": {"tool_name": "patch", "input": {"paths": ["/in/a", "/out/b"]}}, "inside_repository": False,
                              "path_facts": [{"file_path": "/in/a", "inside_repository": True}, {"file_path": "/out/b", "inside_repository": False}]})
    for row in (single, multi):
        approvals.reserve(directory, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    backend = PatchBackend(repo.base)
    backend.approval_write_tools = lambda: frozenset({"Write", "patch"})
    entries = {e["tool_name"]: e for e in approvals.approved_writes(directory, "task-1", backend.approval_write_tools(), lambda row: get("claude").approval_paths(row) or backend.approval_paths(row))}
    assert entries["Write"]["file_path"] == "/out/a.txt" and "file_paths" not in entries["Write"]
    assert entries["patch"]["file_path"] is None
    assert entries["patch"]["file_paths"] == [{"file_path": "/in/a", "inside_repository": True}, {"file_path": "/out/b", "inside_repository": False}]


# --- contract fixtures ------------------------------------------------------------------------------

def test_contract_fixtures_are_byte_identical_to_the_pinned_manifest():
    root = Path(__file__).parents[1] / "contracts" / "v1"
    assert hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest() == "29e83c3e916783a0b1f5be6ee4b6784e9346b9ce74bab131de1ffcfcb46a0590"
    assert (root / "manifest.sha256").read_text().split()[0] == "29e83c3e916783a0b1f5be6ee4b6784e9346b9ce74bab131de1ffcfcb46a0590"
    manifest = json.loads((root / "manifest.json").read_text())
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest


# --- control pipe -----------------------------------------------------------------------------------

@pytest.fixture
def state_workspace(tmp_path_factory: pytest.TempPathFactory) -> Path:
    workspace = tmp_path_factory.mktemp("control-pipe")
    root = workspace / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        yield workspace


RESULT_LINE = '{"type":"result","subtype":"success","is_error":false,"result":"ok"}'


class ControlPipeDouble(ExtraPathsUnsupported):
    """A shell reading Harflow-originated frames on a control pipe, with no live input."""

    name = "control-pipe-double"
    binary = "/bin/sh"
    capabilities = backends.ClaudeBackend.capabilities

    def __init__(self, sink: Path) -> None:
        self.sink = sink
        self.turn_frames = [b'{"frame":"turn-1"}\n']

    def build_start_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", f"head -n 2 > {self.sink}; echo '{RESULT_LINE}'; cat > /dev/null"], stdin_mode=STDIN_DEVNULL, control_pipe=True)

    build_resume_argv = build_start_argv

    def outgoing_frames(self, phase, acc):
        if phase == "initial":
            return [b'{"frame":"initial"}\n']
        frames, self.turn_frames = self.turn_frames, []
        return frames

    def control_pipe_finished(self, acc):
        return acc.result_count >= 1

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False)

    def ingest(self, event, acc):
        ClaudeBackend().ingest(event, acc)

    def normalize(self, event, acc):
        return []

    def classify(self, acc, exit_code):
        return ClaudeBackend().classify(acc, exit_code)

    def interactive_resume_argv(self, session_id, repo_path):
        return [self.binary, "--resume", session_id]


@pytest.mark.asyncio
async def test_a_control_pipe_run_gets_harflow_frames_and_stdin_closes_without_live_input(state_workspace, monkeypatch):
    sink = state_workspace / "stdin.txt"
    backend = ControlPipeDouble(sink)
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    task = await registry.start("hello", state_workspace, backend=backend)
    await asyncio.wait_for(task.done.wait(), 20)
    assert task.live_input is False and task.control_pipe is True
    assert task.status == "completed" and task.input_closed is True
    assert [json.loads(line) for line in sink.read_text().splitlines()] == [{"frame": "initial"}, {"frame": "turn-1"}]
    record = store.read(registry.log_dir, task.task_id)
    assert record is not None and record.live_input is False
    assert task.pump is not None and task.pump.done()


@pytest.mark.asyncio
async def test_a_run_without_a_control_pipe_still_gets_no_stdin(state_workspace, monkeypatch):
    sink = state_workspace / "stdin.txt"

    class NoPipe(ControlPipeDouble):
        name = "no-pipe-double"

        def build_start_argv(self, prompt, **kwargs):
            return Invocation([self.binary, "-c", f"echo '{RESULT_LINE}'; test ! -p /dev/stdin; touch {self.sink}"])

        build_resume_argv = build_start_argv

    backend = NoPipe(sink)
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    task = await registry.start("hello", state_workspace, backend=backend)
    await asyncio.wait_for(task.done.wait(), 20)
    assert task.control_pipe is False and task.pump is None and task.proc.stdin is None


# --- control pipe lifecycle ---------------------------------------------------------------------------

def _double(sink, script, *, finished=True, initial=None, turn=None, name="control-double"):
    """A control-pipe backend whose child runs `script` (a shell fragment)."""
    class Double(ControlPipeDouble):
        def build_start_argv(self, prompt, **kwargs):
            return Invocation([self.binary, "-c", script.replace("SINK", str(sink))], control_pipe=True)

        build_resume_argv = build_start_argv

        def outgoing_frames(self, phase, acc):
            if phase == "initial":
                return initial() if callable(initial) else (initial if initial is not None else [])
            return turn() if callable(turn) else []

        def control_pipe_finished(self, acc):
            return finished and acc.result_count >= 1

    Double.name = name
    return Double(sink)


async def _settled(task, seconds=20):
    await asyncio.wait_for(task.done.wait(), seconds)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["raise", "str", "no-newline"])
async def test_a_failing_initial_frame_stops_the_run_and_closes_stdin(state_workspace, monkeypatch, bad):
    sink = state_workspace / "stdin.txt"

    def initial():
        if bad == "raise":
            raise RuntimeError("hook failed")
        return ["not bytes"] if bad == "str" else [b'{"frame":"no newline"}']

    backend = _double(sink, "cat > SINK", initial=initial, name=f"failing-initial-{bad}")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    task = await registry.start("hello", state_workspace, backend=backend)
    await _settled(task)
    assert task.input_closed is True and task.cancel_requested is True and task.status in {"cancelled", "failed"}
    assert not sink.exists() or sink.read_text() == ""


@pytest.mark.asyncio
async def test_a_backend_without_a_control_pipe_lifetime_is_refused_before_spawn(state_workspace, monkeypatch):
    sink = state_workspace / "stdin.txt"

    class NoLifetime(ControlPipeDouble):
        name = "no-lifetime-double"
        control_pipe_finished = ExtraPathsUnsupported.control_pipe_finished

    backend = NoLifetime(sink)
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    with pytest.raises(ValueError, match="control_pipe_finished"):
        await registry.start("hello", state_workspace, backend=backend)


@pytest.mark.asyncio
@pytest.mark.parametrize("script,case", [
    ("echo '{\"type\":\"result\",\"subtype\":\"error\",\"is_error\":true,\"result\":\"boom\"}'; cat > /dev/null", "error_result"),
    ("exit 0", "process_exit"),
])
async def test_the_control_pump_stops_and_closes_stdin_when_the_run_must_stop(state_workspace, monkeypatch, script, case):
    sink = state_workspace / "stdin.txt"
    backend = _double(sink, script, finished=False, name=f"stops-{case}")
    if case == "error_result":
        backend.ingest = lambda event, acc: setattr(acc, "error_result_seen", True) if event.get("is_error") else None
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    task = await registry.start("hello", state_workspace, backend=backend)
    await _settled(task)
    assert task.input_closed is True and task.pump.done() and task.pump.exception() is None


@pytest.mark.asyncio
async def test_cancelling_a_control_pipe_run_closes_its_pipe_while_finished_is_false(state_workspace, monkeypatch):
    sink = state_workspace / "stdin.txt"
    backend = _double(sink, "cat > SINK", finished=False, name="cancel-double")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    task = await registry.start("hello", state_workspace, backend=backend)
    await asyncio.sleep(0.3)
    assert task.input_closed is False
    await registry.cancel(task)
    await _settled(task)
    assert task.status == "cancelled" and task.input_closed is True and task.pump.done()


@pytest.mark.asyncio
async def test_send_message_on_a_control_pipe_task_is_queued_for_the_next_continuation_never_forwarded(state_workspace, monkeypatch):
    sink = state_workspace / "stdin.txt"
    backend = _double(sink, "cat > SINK", finished=False, name="followup-double")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    task = await registry.start("hello", state_workspace, backend=backend)
    result = await registry.send_message(task, "a user message")
    assert result["delivery"] == "next_continuation"
    await asyncio.sleep(0.5)
    await registry.cancel(task)
    await _settled(task)
    assert "a user message" not in (sink.read_text() if sink.exists() else "")


def _approval_double(sink, script, name):
    from dataclasses import replace

    class ApprovalDouble(ControlPipeDouble):
        capabilities = backends.ClaudeBackend.capabilities._replace(supports_caller_approvals=True)

        def build_start_argv(self, prompt, **kwargs):
            # The session id rides on the command line, as with a real CLI: it is the process identity marker.
            return Invocation([self.binary, "-c", script.replace("SINK", str(self.sink)) + f" # {kwargs.get('session_id')}"], control_pipe=True)

        build_resume_argv = build_start_argv

        def outgoing_frames(self, phase, acc):
            return []

        def validate_caller_approvals(self, enabled, freedom, max_turns, native=False):
            return None

        def qualify_caller_approvals(self):
            return "v1"

        def configure_caller_approvals(self, invocation, freedom, max_turns, version=None, write=False):
            from dataclasses import replace as rep
            return rep(invocation, caller_approvals=True, caller_approval_version=version)

        def approval_stream_version(self, acc):
            return "v1"

        def approval_frame_kind(self, event):
            return {"ask": "request", "withdraw": "cancellation"}.get(event.get("type"))

        def approval_request_key(self, event):
            return "s:" + event["id"] if isinstance(event.get("id"), str) else None

        def decode_approval_request(self, event, qualification=None):
            return {"native_request_id": "s:" + event["id"], "tool_use_id": event["id"], "action": {"tool_name": "cmd", "input": {"command": event["command"]}}}

        def approval_paths(self, decoded):
            return []

        def approval_write_tools(self):
            return frozenset()

        def encode_approval_response(self, request, decision):
            return (json.dumps({"answer": decision, "id": request["native_request_id"]}) + "\n").encode()

    ApprovalDouble.name = name
    return ApprovalDouble(sink)


async def _pending_row(registry, task, seconds=20):
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        rows = approvals.rows(registry.log_dir, task.task_id)
        if rows:
            return rows[0]
        await asyncio.sleep(0.05)
    raise AssertionError("no approval request was admitted")


ASK = "printf '{\"type\":\"ask\",\"id\":\"r1\",\"command\":\"ls\"}\\n'; "


@pytest.mark.asyncio
async def test_a_control_pipe_approval_round_trip_without_live_input(state_workspace, monkeypatch):
    from harflow_mcp import identity
    sink = state_workspace / "answer.txt"
    backend = _approval_double(sink, ASK + f"head -n 1 > SINK; echo '{RESULT_LINE}'; cat > /dev/null", "approval-round-trip")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=identity.own_identity())
    task = await registry.start("hello", state_workspace, backend=backend, caller_approvals=True)
    assert task.live_input is False and task.control_pipe is True
    row = await _pending_row(registry, task)
    assert row["disposition"] == "pending" and row["native_request_id"] == "s:r1"
    approvals.reserve(registry.log_dir, task.task_id, row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    task.pump_wake.set()
    await _settled(task)
    assert json.loads(sink.read_text()) == {"answer": "approve", "id": "s:r1"}
    (final,) = approvals.rows(registry.log_dir, task.task_id)
    assert (final["disposition"], final["delivery"], final["delivered_decision"]) == ("decided", "sent", "approve")
    assert task.status == "completed" and task.input_closed is True


@pytest.mark.asyncio
async def test_cancelling_while_an_approval_is_pending_invalidates_it_and_writes_nothing(state_workspace, monkeypatch):
    from harflow_mcp import identity
    sink = state_workspace / "answer.txt"
    backend = _approval_double(sink, ASK + "cat > SINK", "approval-cancel")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=identity.own_identity())
    task = await registry.start("hello", state_workspace, backend=backend, caller_approvals=True)
    await _pending_row(registry, task)
    await registry.cancel(task)
    await _settled(task)
    (final,) = approvals.rows(registry.log_dir, task.task_id)
    assert final["disposition"] in {"cancelled", "invalidated"} and final["delivery"] in {"failed", "delivery_unknown"}
    assert task.status == "cancelled" and task.input_closed is True
    assert not sink.exists() or sink.read_text() == ""


@pytest.mark.asyncio
async def test_the_pipe_stays_open_while_a_reserved_answer_still_needs_it(state_workspace, monkeypatch):
    """`control_pipe_finished` is true from the first result, but a reserved answer must be written first."""
    from harflow_mcp import identity
    sink = state_workspace / "answer.txt"
    backend = _approval_double(sink, ASK + f"echo '{RESULT_LINE}'; head -n 1 > SINK; cat > /dev/null", "approval-reserved")
    backend.control_pipe_finished = lambda acc: acc.result_count >= 1
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=identity.own_identity())
    task = await registry.start("hello", state_workspace, backend=backend, caller_approvals=True)
    row = await _pending_row(registry, task)
    await asyncio.sleep(0.5)
    assert task.input_closed is False
    approvals.reserve(registry.log_dir, task.task_id, row["request_id"], row["action_digest"], row["owner_revision"], "reject", row["generation"])
    task.pump_wake.set()
    await _settled(task)
    assert json.loads(sink.read_text()) == {"answer": "reject", "id": "s:r1"}


def test_supervised_launch_requests_carry_control_pipe_only_when_set():
    import base64
    from harflow_mcp import task_supervision
    plain = task_supervision.invocation_request(Invocation(["claude", "-p"], stdin_mode="pipe", initial_input=b"hi"))
    assert "control_pipe" not in plain
    claude_shape = {"argv", "stdin_mode", "extra_writable_paths", "native_subagent", "caller_approvals", "caller_write_approvals", "caller_approval_version", "allowed_commands", "allowed_commands_version", "native_settings", "env", "initial_input"}
    assert set(plain) == claude_shape
    request = task_supervision.invocation_request(Invocation(["x"], control_pipe=True))
    assert request["control_pipe"] is True and request["initial_input"] is None
    # The supervisor decodes exactly like this before rebuilding the Invocation.
    prepared = json.loads(json.dumps(request))
    prepared["extra_writable_paths"], prepared["allowed_commands"] = tuple(prepared["extra_writable_paths"]), tuple(prepared["allowed_commands"])
    rebuilt = Invocation(**prepared)
    assert rebuilt.control_pipe is True and rebuilt.stdin_mode == "devnull" and rebuilt.live_input is False
    assert base64 is not None


# --- review round 2 ---------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_turn_frame_failure_never_delivers_a_concurrently_reserved_approval(state_workspace, monkeypatch, caplog):
    """The failing hook reserves a decision and then raises: the failure path must not write that answer."""
    from harflow_mcp import identity
    sink = state_workspace / "answer.txt"
    backend = _approval_double(sink, ASK + "head -n 1 > SINK; cat > /dev/null", "approval-turn-failure")
    holder = {}
    early_turn_seen = asyncio.Event()
    reserved_request_id = None
    local_task = None

    def turn_frames(phase, acc):
        nonlocal reserved_request_id
        if phase != "turn":
            return []
        registry = holder["registry"]
        if "task" not in holder:
            early_turn_seen.set()
            return []
        task = holder["task"]
        rows = approvals.rows(registry.log_dir, task.task_id)
        if not rows:
            return []
        row = rows[0]
        approvals.reserve(registry.log_dir, task.task_id, row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
        reserved_request_id = row["request_id"]
        raise RuntimeError("turn frame hook failed after a decision was reserved")

    backend.outgoing_frames = turn_frames
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=identity.own_identity())
    holder["registry"] = registry
    try:
        local_task = await registry.start("hello", state_workspace, backend=backend, caller_approvals=True)
        await asyncio.wait_for(early_turn_seen.wait(), 5)
        assert early_turn_seen.is_set()
        holder["task"] = local_task
        local_task.pump_wake.set()
        await _settled(local_task)
        (final,) = approvals.rows(registry.log_dir, local_task.task_id)
        # Which of the two closes the row first depends on whether the monitor's own invalidation sees the cancellation.
        assert reserved_request_id == final["request_id"]
        pump_failures = [record for record in caplog.records if record.name == "harflow_mcp.tasks" and "control pump failed" in record.getMessage()]
        assert any(record.exc_info and isinstance(record.exc_info[1], RuntimeError) and str(record.exc_info[1]) == "turn frame hook failed after a decision was reserved" for record in pump_failures)
        assert not any(record.exc_info and isinstance(record.exc_info[1], KeyError) for record in pump_failures)
        assert final["disposition"] in {"invalidated", "cancelled"} and final["delivery"] == "failed" and final["delivered_decision"] is None
        assert not sink.exists() or sink.read_text() == ""
        assert local_task.input_closed is True and local_task.cancel_requested is True
    finally:
        if local_task is not None and not local_task.done.is_set():
            await registry.cancel(local_task)
            await _settled(local_task)


@pytest.mark.asyncio
async def test_snapshots_degrade_when_a_backend_declares_no_approval_write_tools(state_workspace, monkeypatch):
    from dataclasses import replace
    from harflow_mcp import identity
    sink = state_workspace / "answer.txt"
    backend = _approval_double(sink, f"echo '{RESULT_LINE}'; cat > /dev/null", "approval-undeclared-writes")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=identity.own_identity())
    task = await registry.start("hello", state_workspace, backend=backend, caller_approvals=True)
    await _settled(task)

    def undeclared():
        raise UnsupportedCapability("no write tools declared")

    backend.approval_write_tools = undeclared
    task.caller_write_approvals = True
    live = task.snapshot()
    assert live["caller_approved_writes"] == [] and approvals.APPROVED_WRITES_UNAVAILABLE in live["notices"]
    real_read = store.read
    monkeypatch.setattr(store, "read", lambda *a, **k: replace(real_read(*a, **k), caller_write_approvals=True))
    recovered = store.snapshot(registry.log_dir, real_read(registry.log_dir, task.task_id))
    assert recovered["caller_approved_writes"] == [] and approvals.APPROVED_WRITES_UNAVAILABLE in recovered["notices"]


@pytest.mark.asyncio
async def test_a_supervised_launch_of_a_control_pipe_without_a_lifetime_is_refused_before_the_supervisor_starts(state_workspace, monkeypatch):
    from harflow_mcp import task_supervision

    class NoLifetime(ControlPipeDouble):
        name = "supervised-no-lifetime"
        control_pipe_finished = ExtraPathsUnsupported.control_pipe_finished

    backend = NoLifetime(state_workspace / "x")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    started = []
    monkeypatch.setattr(task_supervision.subprocess, "Popen", lambda *a, **k: started.append(a) or (_ for _ in ()).throw(AssertionError("supervisor started")))
    registry = TaskRegistry(log_dir=state_workspace / "state" / "tasks", owner=OWNER)
    with pytest.raises(ValueError, match="control_pipe_finished"):
        await task_supervision.launch(registry, Invocation(["/bin/sh"], control_pipe=True), {"backend": backend, "freedom": "write_in_repo", "repo_path": state_workspace})
    assert started == []


def test_control_context_and_identity_markers_round_trip_through_a_supervised_launch_only_when_set():
    from harflow_mcp import task_supervision
    plain = task_supervision.invocation_request(Invocation(["x"], control_pipe=True))
    assert "control_context" not in plain and "identity_markers" not in plain
    request = task_supervision.invocation_request(Invocation(["x"], control_pipe=True, control_context={"a": 1}, identity_markers=("codex", "app-server")))
    assert request["control_context"] == {"a": 1} and request["identity_markers"] == ["codex", "app-server"]
    with pytest.raises(ValueError, match="control_context requires control_pipe"):
        Invocation(["x"], control_context={"a": 1})
