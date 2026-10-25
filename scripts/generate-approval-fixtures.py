#!/usr/bin/env python3
"""Regenerate the canonical approval fixtures in contracts/v1 from actual `harflow-ctl` output.

The approval-list, approval-get, approval-respond and status documents are produced by calling `ctl.main` against a
ledger that production code wrote, inside an isolated temporary HOME and state root, with a seeded clock and request ids.

    uv run python scripts/generate-approval-fixtures.py            # rewrite contracts/v1
    uv run python scripts/generate-approval-fixtures.py --check    # fail if contracts/v1 differs from a fresh, isolated run
    .venv/bin/python scripts/generate-approval-fixtures.py --provenance /tmp/provenance.json

What is production and what is controlled:

* Admission uses the production backend decoders (`claude_approvals.decode`, `codex_app_server.decode`) through
  `approval_runtime.observe`, including path snapshots and protected-path checks.
* Decisions go through `ctl approval-respond`; delivery goes through `approval_runtime.pump`.
* Controlled facts: the wall clock, request-id generation, process-liveness (`identity.identity_check`), caller-process
  detection (`lineage.detect_caller_detail`, so the generator needs no `ps`) and the pipe write (`tasks._write_stdin`,
  no agent process exists). The `sending` state is written by the same `approvals.update` call the pump makes just
  before its write.
* Synthetic fixtures (named `*-synthetic.json`) are mechanical transformations of live documents, for compatibility
  tests: a legacy error without `data.reason`, an unknown reason, the reserved reasons `incomplete` and `unsupported`
  (no production ctl path raises them) and rows with the optional fields omitted. They carry no live provenance.
* Redaction: the temporary base directory becomes `/workspace`; every `action_digest` is recomputed from the redacted
  action and substituted in every document, so list rows and get documents stay consistent. Ids and times need no
  mapping: they are seeded. Live diagnostic references to legacy environment aliases are deterministically redacted
  to distinct placeholders; affected fixtures record `legacy_environment_alias_redaction` in provenance metadata.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "contracts" / "v1"
PREFIX = "ctl-approval-"
CAPABILITY = "caller-approvals.monitor-respond.v1"
EPOCH = 1_792_000_000.0
OWNER = {"pid": 5678, "start_time": "bridge-start", "markers": []}
CLAUDE_VERSION = "2.1.310"
CODEX_VERSION = "0.163.0"
THREAD, TURN = "thread-1", "turn-1"
FIXTURE_HOME = "/tmp/harflow-fixture-home"
WORKSPACE = "/workspace"
OWNED_EXTRA = ("version-info.json",)
LEGACY_ENVIRONMENT_ALIASES = {"PB_TASK_ID": "<legacy task marker>", "PB_DEPTH": "<legacy depth marker>"}


class Clock:
    def __init__(self) -> None:
        self.now = EPOCH

    def __call__(self) -> float:
        return self.now

    def tick(self, seconds: float = 1.0) -> None:
        self.now += seconds


def canonical_bytes(document) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")


class World:
    """One isolated engine state with the controlled facts installed."""

    def __init__(self, base: Path) -> None:
        self.base = Path(os.path.realpath(base))
        self.docs: dict[str, dict] = {}
        self.meta: dict[str, dict] = {}
        self.clock = Clock()
        self.facts = {"identity": "alive", "undecidable": None}
        self.counter = 0
        self.uuids = 0
        self.tasks: dict[str, SimpleNamespace] = {}

    # --- environment ------------------------------------------------------------------------------------

    def enter(self, stack: contextlib.ExitStack) -> None:
        from harflow_mcp import ctl, identity, lineage, server, state_ownership, store, approvals, approval_runtime
        from harflow_mcp.state_layout import resolve_layout
        self.ctl, self.identity, self.server, self.store = ctl, identity, server, store
        self.approvals, self.approval_runtime = approvals, approval_runtime
        base = self.base
        self.repo, self.home = base / "repo", base / "home"
        self.root = base / "state"
        self.tasks_dir = self.root / "tasks"
        for path in (self.repo, self.home, base / "config", base / "codex-home", base / "tmp"):
            path.mkdir(parents=True, exist_ok=True)
        environment = {"HOME": str(self.home), "CLAUDE_CONFIG_DIR": str(base / "config"), "CODEX_HOME": str(base / "codex-home"),
                       "TMPDIR": str(base / "tmp"), "HARFLOW_STATE_ROOT": str(self.root), "XDG_CONFIG_HOME": str(base / "config"),
                       "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull}
        stack.enter_context(mock.patch.dict(os.environ, environment))
        for key in list(os.environ):
            if key.startswith("GIT_") and key not in environment:
                os.environ.pop(key)
        subprocess.run(["git", "init", "-q", "--template=", str(self.repo)], check=True)
        for key in ("HARFLOW_TASK_ID", "HARFLOW_DEPTH", "HARFLOW_STATE_ROOT_OVERRIDE", *LEGACY_ENVIRONMENT_ALIASES):
            os.environ.pop(key, None)
        stack.enter_context(mock.patch("time.time", self.clock))
        clock = self.clock

        class SeededDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls.fromtimestamp(clock(), tz=tz)

        stack.enter_context(mock.patch.object(store, "datetime", SeededDatetime))
        stack.enter_context(mock.patch("uuid.uuid4", self.next_uuid))
        stack.enter_context(mock.patch.object(identity, "identity_check", lambda _identity: self.facts["identity"]))
        stack.enter_context(mock.patch.object(lineage, "detect_caller_detail", lambda *_a, **_k: SimpleNamespace(caller=None, undecidable=self.facts["undecidable"])))
        stack.enter_context(mock.patch.object(ctl, "default_log_dir", lambda: self.tasks_dir))
        stack.enter_context(state_ownership.state_session(resolve_layout(root=self.root)))
        self.tasks_dir.mkdir(parents=True, exist_ok=True)
        self.hub = Hub(self)
        previous = server._registry
        server._registry = self.hub
        stack.callback(setattr, server, "_registry", previous)

    def next_uuid(self) -> uuid.UUID:
        self.uuids += 1
        return uuid.UUID(int=self.uuids)

    @contextlib.contextmanager
    def environ(self, **values):
        with mock.patch.dict(os.environ, values):
            yield

    # --- recording --------------------------------------------------------------------------------------

    def record(self, name: str, document: dict, source: str, note: str, capability: bool = False) -> None:
        assert name not in self.docs, name
        self.docs[name] = document
        self.meta[name] = {"provenance": "synthetic" if source == "synthetic" else "live", "source": source, "note": note,
                           "requires_capability": capability, "transformations": []}

    def call(self, *argv: str, name: str | None = None, note: str = "", capability: bool = False) -> tuple[int, dict]:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = self.ctl.main([*argv, "--json"])
        lines = [line for line in out.getvalue().splitlines() if line.strip()]
        if not lines:
            raise RuntimeError(f"ctl {argv} printed no document: {err.getvalue()}")
        document = json.loads(lines[-1])
        if name is not None:
            self.record(PREFIX + name + ".json", document, "ctl " + " ".join(argv), note, capability)
        return code, document

    def run(self, coroutine):
        return asyncio.run(coroutine)

    # --- state ------------------------------------------------------------------------------------------

    def new_task(self, task_id: str, backend: str, owner: str = "caller", *, state=None, ctx=None) -> SimpleNamespace:
        from harflow_mcp.backends import Accumulator
        self.counter += 1
        version = CLAUDE_VERSION if backend == "claude" else CODEX_VERSION
        session = f"session-{self.counter}"
        generation = f"generation-{task_id}"
        self.store.write(self.tasks_dir, self.store.TaskRecord(
            task_id=task_id, backend=backend, session_id=session, repo_path=str(self.repo), started_at=datetime.fromtimestamp(EPOCH, timezone.utc).isoformat(), status="running",
            caller_approvals=True, caller_write_approvals=True, caller_approval_version=version, approval_generation=generation, start_time="task-start",
            owner=OWNER, markers=["task-marker"], interaction_owner=owner))
        task = SimpleNamespace(
            task_id=task_id, backend=backend, proc=SimpleNamespace(pid=os.getpid(), returncode=None), start_time="task-start", markers=("task-marker",), owner=dict(OWNER),
            approval_generation=generation, session_id=session, repo_path=self.repo, freedom="write_in_repo", caller_approvals=True, caller_write_approvals=True,
            caller_approval_version=version, protected_paths=None, interaction_owner=owner, cancel_requested=False, input_closed=False, bridge_notices=[],
            approval_pending=False, approval_receiving=False, approval_view={"pending_count": 0, "interaction_state": None, "requests": []},
            pump_wake=asyncio.Event(), acc=Accumulator())
        if backend == "claude":
            task.acc.stream_state["claude_code_version"] = CLAUDE_VERSION
        else:
            task.acc.stream_state.update(control_context=ctx, app_server=state)
        self.tasks[task_id] = task
        return task

    def rows(self, task_id: str) -> list[dict]:
        return self.approvals.rows(self.tasks_dir, task_id)

    def find(self, task_id: str, **match) -> dict:
        (found,) = [r for r in self.rows(task_id) if all(r.get(k) == v for k, v in match.items())]
        return found

    def admit(self, task, event: dict, backend: str) -> bool:
        from harflow_mcp.backends import get
        self.clock.tick()
        return self.run(self.approval_runtime.observe(self.hub, task, get(backend), event))

    def claude(self, task, tool: str, tool_input: dict, *, reason: str | None = None, kind: str | None = None) -> dict:
        self.counter += 1
        n = self.counter
        request = {"subtype": "can_use_tool", "tool_name": tool, "tool_use_id": f"tool-{n}", "input": tool_input}
        if reason is not None:
            request["decision_reason"] = reason
        if kind is not None:
            request["decision_reason_type"] = kind
        assert self.admit(task, {"type": "control_request", "request_id": f"native-{n}", "request": request}, "claude") is True
        return self.find(task.task_id, native_request_id=f"native-{n}")

    def pump(self, task, written: bool = True) -> None:
        with mock.patch("harflow_mcp.tasks._write_stdin", lambda _task, _wire: written):
            self.run(self.approval_runtime.pump(self.hub, task))

    def get(self, task_id: str, row: dict, name: str | None = None, *, note: str = "", capability: bool = False, monitor: bool = False) -> dict:
        argv = ["approval-get", task_id, row["request_id"], *(["--monitor"] if monitor else [])]
        code, document = self.call(*argv, name=name, note=note, capability=capability)
        assert code == 0, document
        return document["result"]

    def respond(self, task_id: str, row: dict, decision: str, *, monitor: bool = False, revision: int | None = None, name: str | None = None, note: str = "",
                capability: bool = False, expect: int = 0) -> dict:
        row = self.find(task_id, request_id=row["request_id"])
        argv = ["approval-respond", task_id, row["request_id"], "--action-digest", row["action_digest"], "--owner-revision",
                str(row["owner_revision"] if revision is None else revision), "--decision", decision, *(["--monitor"] if monitor else [])]
        code, document = self.call(*argv, name=name, note=note, capability=capability)
        assert (code == 0) == (expect == 0), (argv, document)
        return document


class Hub:
    """The registry seam `ctl` installs: log directory, live tasks and the caller-identity fact (no managed caller)."""

    def __init__(self, world: World) -> None:
        from harflow_mcp.state_layout import resolve_layout
        self.world = world
        self.log_dir = world.tasks_dir
        self.state_layout = resolve_layout(log_dir=world.tasks_dir)

    def persist(self, _task) -> None:
        return None

    def _stop_native_transport(self, _task) -> None:
        return None

    async def _mutation_caller(self):
        return None

    def get(self, task_id: str):
        return self.world.tasks.get(task_id)


# --- Codex admission state (the same frames the production state machine consumes) ---------------------------

def codex_context(repo: Path) -> dict:
    cwd = os.path.realpath(repo)
    return {"transport": None, "version": CODEX_VERSION, "cwd": cwd, "git_root": cwd, "freedom": "write_in_repo", "network": False, "model": None, "effort": None,
            "prompt": "do the thing", "thread_id": None, "write": True}


def codex_state(repo: Path):
    from harflow_mcp.backends import codex_app_server as app
    ctx = codex_context(repo)
    ctx["transport"] = app.TRANSPORT
    state = app.State(ctx)

    def answer(method, result):
        key = next(k for k, m in state.pending.items() if m == method)
        app.step(state, {"id": app.untyped(key), "result": result})

    app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))
    answer("initialize", {"userAgent": f"harflow/{CODEX_VERSION} (Mac OS) fake"})
    answer("thread/start", {"thread": {"id": THREAD, "cliVersion": CODEX_VERSION}, "approvalPolicy": "on-request", "approvalsReviewer": "user", "cwd": ctx["cwd"],
                            "runtimeWorkspaceRoots": [ctx["cwd"]], "sandbox": {"type": "workspaceWrite", "writableRoots": [], "networkAccess": False}})
    answer("hooks/list", {"data": [{"cwd": ctx["cwd"], "hooks": [], "warnings": [], "errors": []}]})
    answer("turn/start", {"turn": {"id": TURN, "status": "inProgress"}})
    state.outbox.clear()
    return ctx, state, app


def codex_command(world: World, task, app, item: str, rid: int, command: str, reason: str | None = None) -> dict:
    params = {"threadId": THREAD, "turnId": TURN, "itemId": item, "startedAtMs": 5, "kind": "command", "command": command, "cwd": str(world.repo),
              "commandActions": [{"type": "unknown", "command": command}], "availableDecisions": ["accept", "cancel"]}
    if reason is not None:
        params["reason"] = reason
    assert world.admit(task, {"id": rid, "method": app.COMMAND_REQUEST, "params": params}, "codex") is True
    return world.find(task.task_id, tool_use_id=item)


def codex_file_change(world: World, task, app, state, item: str, rid: int, changes: list[dict]) -> dict:
    app.step(state, {"method": "item/started", "params": {"threadId": THREAD, "turnId": TURN, "item": {"type": "fileChange", "id": item, "changes": changes, "status": "inProgress"}}})
    params = {"threadId": THREAD, "turnId": TURN, "itemId": item, "startedAtMs": 9, "reason": None, "grantRoot": None}
    assert world.admit(task, {"id": rid, "method": app.FILE_CHANGE_REQUEST, "params": params}, "codex") is True
    return world.find(task.task_id, tool_use_id=f"fileChange:{item}")


# --- scenarios ------------------------------------------------------------------------------------------------

def build(world: World) -> None:
    repo = world.repo
    (repo / "src").mkdir()
    (repo / "work").mkdir()
    approvals = world.approvals

    # task-open: pending rows of every Claude action shape, one decided-but-undelivered row.
    open_task = world.new_task("task-open", "claude")
    bash = world.claude(open_task, "Bash", {"command": "git status --short"}, reason="Command reads repository state outside the sandbox", kind="other")
    bare = world.claude(open_task, "Bash", {"command": "ls src"})
    write = world.claude(open_task, "Write", {"file_path": str(repo / "src" / "a.txt"), "content": "hello\n"})
    edit = world.claude(open_task, "Edit", {"file_path": str(repo / "src" / "b.txt"), "old_string": "alpha", "new_string": "beta"})
    answered = world.claude(open_task, "Bash", {"command": "echo answered"})
    world.get("task-open", bash, "get-claude-bash", note="Claude Bash with decision_reason and decision_reason_type")
    world.get("task-open", bare, "get-claude-bash-no-reason", note="Claude Bash; decision_reason and decision_reason_type are retained as null")
    world.get("task-open", write, "get-claude-write", note="Claude Write")
    world.get("task-open", edit, "get-claude-edit", note="Claude Edit")
    world.respond("task-open", answered, "approve", monitor=True, name="respond-monitor", capability=True,
                  note="The Monitor channel decides a caller-owned request: responder monitor, decided_by caller")
    world.get("task-open", answered, "state-reserved", capability=True, note="Decision accepted, not yet delivered: delivery reserved, disposition pending, still counted in pending_count")
    world.call("approval-list", "--task-id", "task-open", name="list-pending", note="Four awaiting requests and one decided-but-undelivered request")
    _, page1 = world.call("approval-list", "--task-id", "task-open", "--limit", "2", name="list-page-1", note="First page, next_cursor present")
    cursor = page1["result"]["next_cursor"]
    _, page2 = world.call("approval-list", "--task-id", "task-open", "--limit", "2", "--cursor", cursor, name="list-page-2", note="Middle page")
    world.call("approval-list", "--task-id", "task-open", "--limit", "2", "--cursor", page2["result"]["next_cursor"], name="list-page-3", note="Last page, next_cursor null")
    code, _ = world.call("status", "task-open", name="status-awaiting", note="Full task status: approval_summary.pending_count counts the reserved row, awaiting_decision_count does not")
    assert code == 0

    # Typed refusals on task-open.
    world.respond("task-open", bash, "approve", revision=bash["owner_revision"] + 5, expect=1, name="error-not-current", note="Stale owner revision")
    again = world.find("task-open", request_id=answered["request_id"])
    world.respond("task-open", answered, "approve", revision=again["owner_revision"], expect=1, name="error-already-answered", note="Second decision on a reserved row")
    world.facts["identity"] = "dead"
    world.respond("task-open", bare, "approve", expect=1, name="error-ownership-lost", note="The owning process is not alive: no decision applied")
    world.facts["identity"] = "alive"
    with world.environ(HARFLOW_DEPTH="1"):
        code, document = world.call("approval-get", "task-open", bash["request_id"], "--monitor", name="error-unauthorized-agent", note="Monitor channel refused for a worker-depth environment")
        assert code == 1
    with world.environ(HARFLOW_TASK_ID="task-open"):
        code, document = world.call("approval-get", "task-open", bash["request_id"], name="error-unauthorized-managed-task", note="A managed task may not read approvals")
        assert code == 1
    world.facts["undecidable"] = "process table unreadable"
    world.call("approval-get", "task-open", bash["request_id"], "--monitor", name="error-caller-undecidable-approval", note="Monitor channel refused because the caller could not be established (approval_error form)")
    world.facts["undecidable"] = None
    with world.environ(HARFLOW_TASK_ID="stale-task"):
        code, _ = world.call("approval-respond", "task-open", bash["request_id"], "--action-digest", bash["action_digest"], "--owner-revision", str(bash["owner_revision"]), "--decision", "approve",
                             name="error-caller-undecidable-global", note="Global ctl mutation guard: top-level caller_undecidable error, not approval_error")
        assert code == 1
    code, _ = world.call("approval-list", "--task-id", "task-open", "--limit", "99", name="error-generic-no-reason", note="approval_error without data.reason (plain ValueError)")
    assert code == 1

    # task-history: delivery and settlement outcomes.
    history = world.new_task("task-history", "claude")
    h1 = world.claude(history, "Bash", {"command": "echo approved"})
    h2 = world.claude(history, "Bash", {"command": "echo rejected"})
    h3 = world.claude(history, "Write", {"file_path": str(repo / "work" / "pre-commit"), "content": "#!/bin/sh\n"})
    h4 = world.claude(history, "Write", {"file_path": str(repo / ".git" / "hooks" / "pre-commit"), "content": "#!/bin/sh\n"})
    h5 = world.claude(history, "Bash", {"command": "echo sending"})
    h6 = world.claude(history, "Bash", {"command": "echo unknown"})
    world.respond("task-history", h1, "approve", name="respond-caller", note="Plain caller decision on a caller-owned request: responder caller")
    world.pump(history)
    world.respond("task-history", h2, "reject", monitor=True)
    world.pump(history)
    world.respond("task-history", h3, "approve", monitor=True)
    shutil_rmdir(repo / "work")
    (repo / "work").symlink_to(repo / ".git" / "hooks")
    world.pump(history)
    world.respond("task-history", h6, "approve", monitor=True)
    world.pump(history, written=False)
    world.respond("task-history", h5, "approve")
    current = world.find("task-history", request_id=h5["request_id"])
    approvals.update(world.tasks_dir, "task-history", current["request_id"], current["generation"], "reserved", delivery="sending")
    world.get("task-history", h1, "state-sent-approved", note="Approved and delivered: delivery sent, delivered_decision approve, execution_outcome unknown until observed")
    world.get("task-history", h2, "state-sent-rejected-monitor", capability=True, note="Rejected from the Monitor channel and delivered")
    world.get("task-history", h3, "state-system-rejected-delivery", capability=True,
              note="Approved by the Monitor, then rejected by Harflow's delivery-time path check: decision approve, effective_decision reject, system_rejection path_changed_since_admission, disposition system_rejected")
    world.get("task-history", h4, "state-system-rejected-admission", note="Rejected by Harflow at admission (protected path): decision null, decided_by harflow")
    world.get("task-history", h5, "state-sending", note="Send reservation written, pipe write not yet confirmed")
    world.get("task-history", h6, "state-delivery-unknown", note="Pipe write failed: delivery_unknown, delivered_decision null, never resent")
    world.respond("task-history", h1, "approve", expect=1, name="error-settled", note="Decision on a request that already settled")
    world.call("approval-list", "--task-id", "task-history", "--limit", "32", name="list-history", note="Retained history: sent, system-rejected, sending and delivery_unknown rows")

    # task-cancel, task-stale, task-unsupported: invalidation dispositions.
    cancel = world.new_task("task-cancel", "claude")
    cancelled = world.claude(cancel, "Bash", {"command": "echo cancel"})
    cancel.cancel_requested = True
    world.pump(cancel)
    world.get("task-cancel", cancelled, "state-cancelled", note="Task cancellation closed a pending request: disposition cancelled, delivery failed")
    stale = world.new_task("task-stale", "claude")
    invalidated = world.claude(stale, "Bash", {"command": "echo invalidated"})
    stale.input_closed = True
    world.pump(stale)
    world.get("task-stale", invalidated, "state-invalidated", note="The input pipe closed under a pending request: disposition invalidated, delivery failed")
    unsupported = world.new_task("task-unsupported", "claude")
    world.counter += 1
    assert world.admit(unsupported, {"type": "control_request", "request_id": f"native-{world.counter}", "request": {"subtype": "can_use_tool", "tool_name": "WebFetch", "tool_use_id": f"tool-{world.counter}", "input": {"url": "https://example.test"}}}, "claude")
    audit = world.find("task-unsupported", native_request_id=f"native-{world.counter}")
    world.get("task-unsupported", audit, "state-unsupported", note="Unsupported permission request recorded as a bounded audit row: tool unsupported, disposition unsupported")

    # task-monitor: a Monitor-owned request.
    monitor = world.new_task("task-monitor", "claude", owner="monitor")
    owned = world.claude(monitor, "Bash", {"command": "echo monitor-owned"})
    world.respond("task-monitor", owned, "approve", expect=1, name="error-unauthorized-owner", note="A plain caller may not decide a Monitor-owned request")
    world.respond("task-monitor", owned, "approve", monitor=True, name="respond-monitor-owned", note="The Monitor decides its own request")

    # task-codex: commandExecution and fileChange shapes.
    ctx, state, app = codex_state(repo)
    codex = world.new_task("task-codex", "codex", state=state, ctx=ctx)
    command = codex_command(world, codex, app, "call-1", 0, "/bin/zsh -lc 'touch generated.txt'", reason="Command writes outside the sandbox")
    plain = codex_command(world, codex, app, "call-2", 1, "/bin/zsh -lc 'ls'")
    created = str(repo / "src" / "new.txt")
    moved_from, moved_to = str(repo / "src" / "old.txt"), str(repo / "src" / "renamed.txt")
    patch = codex_file_change(world, codex, app, state, "p1", 5, [{"path": created, "kind": {"type": "add"}, "diff": "hello\n"}])
    moved = codex_file_change(world, codex, app, state, "p2", 6, [{"path": moved_from, "kind": {"type": "update", "move_path": moved_to}, "diff": "@@\n-x\n+y\n"}])
    world.get("task-codex", command, "get-codex-command", note="Codex commandExecution with its reason retained in decision_reason and decision_reason_type")
    world.get("task-codex", patch, "get-codex-file-change", note="Codex fileChange with every path, kind and diff")
    world.get("task-codex", moved, "get-codex-file-change-move", note="Codex fileChange update with move_path")
    world.call("approval-list", "--task-id", "task-codex", name="list-codex", note="Codex commandExecution and fileChange rows")
    world.respond("task-codex", plain, "reject", monitor=True)
    world.pump(codex)
    world.get("task-codex", plain, "state-codex-sent", capability=True, note="Codex request rejected from the Monitor channel and delivered")

    # task-expiry: runs last, because the clock moves past every deadline.
    expiry = world.new_task("task-expiry", "claude")
    override = world.claude(expiry, "Bash", {"command": "echo override"})
    silent = world.claude(expiry, "Bash", {"command": "echo silent"})
    late = world.claude(expiry, "Bash", {"command": "echo late"})
    world.respond("task-expiry", override, "approve")
    world.clock.tick(approvals.REQUEST_SECONDS + 100)
    world.respond("task-expiry", late, "approve", expect=1, name="error-expired", note="Decision after the request deadline")
    world.pump(expiry)
    world.get("task-expiry", override, "state-expired-override", note="Approve accepted, then the deadline passed: decision approve, effective_decision reject, expiry_rejection true, disposition expired")
    world.get("task-expiry", silent, "state-expired-unanswered", note="Unanswered at the deadline: decision null, effective_decision reject, disposition expired")


def shutil_rmdir(path: Path) -> None:
    path.rmdir()


# --- synthetic compatibility transformations --------------------------------------------------------------------

def add_synthetic(world: World) -> None:
    def derived(source: str, name: str, mutate, note: str) -> None:
        document = copy.deepcopy(world.docs[PREFIX + source + ".json"])
        mutate(document)
        world.record(PREFIX + name + ".json", document, "synthetic", f"Transformation of {PREFIX + source}.json: {note}")

    def strip_reason(document):
        document["error"].pop("data", None)

    def set_reason(reason):
        def mutate(document):
            document["error"]["data"] = {"reason": reason}
        return mutate

    derived("error-not-current", "error-legacy-no-reason-synthetic", strip_reason, "data.reason removed, as an older engine reports every approval refusal")
    derived("error-not-current", "error-unknown-reason-synthetic", set_reason("future_reason"), "an unknown reason a newer engine could add")
    derived("error-not-current", "error-reason-incomplete-synthetic", set_reason("incomplete"), "reserved reason: no production path raises it")
    derived("error-not-current", "error-reason-unsupported-synthetic", set_reason("unsupported"), "reason with no ctl path: argparse rejects the inputs that raise it")

    def omit_optional(document):
        target = document["result"]
        for row in target["requests"] if "requests" in target else [target]:
            for key in ("decision_reason", "decision_reason_type", "responder"):
                row.pop(key, None)

    derived("get-claude-bash", "get-optional-fields-omitted-synthetic", omit_optional, "decision_reason, decision_reason_type and responder omitted")
    derived("list-pending", "list-optional-fields-omitted-synthetic", omit_optional, "decision_reason, decision_reason_type and responder omitted from every row")


# --- redaction and digests ---------------------------------------------------------------------------------------

def redact(value, bases: tuple[str, ...]):
    if isinstance(value, str):
        for base in bases:
            value = value.replace(base, WORKSPACE)
        return value
    if isinstance(value, list):
        return [redact(item, bases) for item in value]
    if isinstance(value, dict):
        return {key: redact(item, bases) for key, item in value.items()}
    return value


def walk(value):
    yield value
    if isinstance(value, dict):
        for item in value.values():
            yield from walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from walk(item)


def substitute(value, mapping: dict[str, str]):
    if isinstance(value, str):
        return mapping.get(value, value)
    if isinstance(value, list):
        return [substitute(item, mapping) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, mapping) for key, item in value.items()}
    return value


def finalize(world: World) -> dict[str, dict]:
    from harflow_mcp import approvals
    bases = tuple(dict.fromkeys((str(world.base), os.path.realpath(world.base))))
    documents = {name: redact(document, bases) for name, document in world.docs.items()}
    for name, document in documents.items():
        error = document.get("error")
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            original = error["message"]
            for alias, placeholder in LEGACY_ENVIRONMENT_ALIASES.items():
                error["message"] = error["message"].replace(alias, placeholder)
            if error["message"] != original:
                world.meta[name]["transformations"].append("legacy_environment_alias_redaction")
    mapping: dict[str, str] = {}
    original_digests = {}
    for original in world.docs.values():
        for node in walk(original):
            if isinstance(node, dict) and isinstance(node.get("action"), dict) and isinstance(node.get("action_digest"), str):
                original_digests[node["action_digest"]] = redact(node["action"], bases)
    for old, action in original_digests.items():
        mapping[old] = approvals.digest(action)
    documents = {name: substitute(document, mapping) for name, document in documents.items()}
    for name, document in documents.items():
        for node in walk(document):
            if isinstance(node, dict) and isinstance(node.get("action_digest"), str):
                assert node["action_digest"] in set(mapping.values()), f"{name}: action_digest without a get document to recompute from"
            if isinstance(node, dict) and isinstance(node.get("action"), dict) and "action_digest" in node:
                assert node["action_digest"] == approvals.digest(node["action"]), f"{name}: digest does not match its action"
        text = json.dumps(document)
        assert not any(base in text for base in bases), f"{name}: temporary path leaked"
        assert "path_snapshot" not in text and "bridge_owner" not in text and "process_identity" not in text, f"{name}: private ledger field leaked"
    return documents


# --- output ----------------------------------------------------------------------------------------------------------

def version_info_document() -> dict:
    from harflow_mcp import wire_contracts
    document = wire_contracts.version_info({"HOME": FIXTURE_HOME})
    document.update(os="darwin", architecture="arm64", state_root=f"{FIXTURE_HOME}/.harflow")
    return document


def manifest_bytes(manifest: dict) -> bytes:
    """Preserve the bundle's canonical insertion-order, indented manifest format."""
    return (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def write_output(output: Path, documents: dict[str, dict], meta: dict[str, dict]) -> None:
    from harflow_mcp import wire_contracts
    from harflow_mcp.wire_contracts import CTL_FORMAT, VERSION_INFO_FORMAT, PRODUCT_VERSION, validate_envelope
    output.mkdir(parents=True, exist_ok=True)
    source_manifest = FIXTURES / "manifest.json"
    raw_manifest = source_manifest.read_bytes()
    manifest = json.loads(raw_manifest)
    for name in [n for n in manifest["cases"] if n.startswith(PREFIX)]:
        manifest["cases"].pop(name)
        manifest["files"].pop(name, None)
    for stale in output.glob(PREFIX + "*.json"):
        stale.unlink()
    written: dict[str, bytes] = {}
    for name, document in sorted(documents.items()):
        validate_envelope(document, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
        written[name] = canonical_bytes(document)
        manifest["cases"][name] = {"encoding": "json", "expected_product_version": PRODUCT_VERSION, "format": CTL_FORMAT,
                                   "required_capabilities": [CAPABILITY] if meta[name]["requires_capability"] else [], "valid": True}
    info = version_info_document()
    validate_envelope(info, VERSION_INFO_FORMAT, live=True, expected_version=PRODUCT_VERSION)
    assert CAPABILITY in info["capabilities"] and info["product_version"] == PRODUCT_VERSION == wire_contracts.PRODUCT_VERSION
    written["version-info.json"] = canonical_bytes(info)
    for name, payload in written.items():
        (output / name).write_bytes(payload)
        manifest["files"][name] = hashlib.sha256(payload).hexdigest()
    # Files outside this generator's ownership keep their recorded hashes; verify they are untouched.
    for name, recorded in manifest["files"].items():
        if name not in written and name != "manifest.json":
            actual = hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest()
            if actual != recorded:
                raise SystemExit(f"{name} differs from its recorded hash; the generator does not own it")
    payload = manifest_bytes(manifest)
    (output / "manifest.json").write_bytes(payload)
    (output / "manifest.sha256").write_text(f"{hashlib.sha256(payload).hexdigest()}  manifest.json\n")


def generate(output: Path, provenance: Path | None) -> None:
    with tempfile.TemporaryDirectory(prefix="harflow-approval-fixtures-") as base:
        world = World(Path(base))
        with contextlib.ExitStack() as stack:
            world.enter(stack)
            build(world)
            add_synthetic(world)
        documents = finalize(world)
    write_output(output, documents, world.meta)
    if provenance is not None:
        provenance.write_text(json.dumps({"generator": "scripts/generate-approval-fixtures.py", "capability": CAPABILITY, "fixtures": dict(sorted(world.meta.items()))}, indent=2, sort_keys=True) + "\n")


def owned_files(directory: Path) -> dict[str, bytes]:
    names = {p.name for p in directory.glob(PREFIX + "*.json")} | set(OWNED_EXTRA) | {"manifest.json", "manifest.sha256"}
    return {name: (directory / name).read_bytes() for name in sorted(names) if (directory / name).exists()}


def check() -> int:
    """Generate twice in separate processes into temporary directories and compare both with contracts/v1."""
    with tempfile.TemporaryDirectory(prefix="harflow-approval-check-") as scratch:
        outputs = []
        for index in (1, 2):
            target = Path(scratch) / f"run{index}"
            subprocess.run([sys.executable, str(Path(__file__).resolve()), "--output", str(target)], check=True)
            outputs.append(owned_files(target))
        committed = owned_files(FIXTURES)
    problems = []
    if outputs[0] != outputs[1]:
        problems.append("two fresh runs differ (non-deterministic): " + ", ".join(sorted(n for n in set(outputs[0]) | set(outputs[1]) if outputs[0].get(n) != outputs[1].get(n))))
    if outputs[0] != committed:
        problems.append("contracts/v1 differs from a fresh run: " + ", ".join(sorted(n for n in set(outputs[0]) | set(committed) if outputs[0].get(n) != committed.get(n))))
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=FIXTURES, help="directory to write (default: contracts/v1)")
    parser.add_argument("--check", action="store_true", help="verify contracts/v1 is exactly reproducible; writes nothing there")
    parser.add_argument("--provenance", type=Path, help="also write the fixture provenance mapping to this JSON file")
    arguments = parser.parse_args()
    if arguments.check:
        return check()
    generate(arguments.output, arguments.provenance)
    return 0


if __name__ == "__main__":
    sys.exit(main())
