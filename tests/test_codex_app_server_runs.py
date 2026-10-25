"""End-to-end Codex caller approvals against the scripted fake app-server (`tests/fake_codex_app_server.py`).

A real `TaskRegistry` spawns a stand-in `codex` found on PATH, so argv, pipes, the approval ledger, the pump, process
cleanup and classification all run for real; only the Codex binary is scripted. macOS only, like the qualification.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approvals, identity, state_ownership
from harflow_mcp.backends import codex_app_server as app
from harflow_mcp.backends import get
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="Codex caller approvals are qualified on macOS only")

FAKE = Path(__file__).parent / "fake_codex_app_server.py"
TRANSCRIPTS = Path(__file__).parent / "fixtures" / "codex_app_server" / "transcripts"
THREAD = "11111111-2222-7333-8444-555555555555"
TURN = "99999999-8888-7777-6666-555555555555"


class World:
    def __init__(self, base: Path, registry: TaskRegistry, monkeypatch) -> None:
        self.base, self.registry, self.monkeypatch = base, registry, monkeypatch
        self.repo = base / "repo"
        self.log = base / "fake.log"
        self.scenario_path = base / "scenario.json"

    def scenario(self, **scenario) -> None:
        self.scenario_path.write_text(json.dumps(scenario))
        self.monkeypatch.setenv("FAKE_CODEX_SCENARIO", str(self.scenario_path))

    async def start(self, prompt="do it", **kwargs):
        return await self.registry.start(prompt, self.repo, backend=get("codex"), caller_approvals=True, **kwargs)

    def frames(self) -> list[dict]:
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def client(self) -> list[dict]:
        return [f for f in self.frames() if "argv" not in f and "eof" not in f and "spawned" not in f]

    def methods(self) -> list[str]:
        return [f["method"] for f in self.client() if "method" in f]

    def rows(self, task) -> list[dict]:
        return approvals.rows(self.registry.log_dir, task.task_id)

    async def pending(self, task, count=1, seconds=20):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            rows = [r for r in self.rows(task) if r["disposition"] == "pending" and r["delivery"] == "pending"]
            if len(rows) >= count:
                return rows
            await asyncio.sleep(0.05)
        raise AssertionError(f"no pending approval: {self.rows(task)}")

    def decide(self, task, row, decision):
        approvals.reserve(self.registry.log_dir, task.task_id, row["request_id"], row["action_digest"], row["owner_revision"], decision, row["generation"])
        task.pump_wake.set()

    async def settled(self, task, seconds=30):
        await asyncio.wait_for(task.done.wait(), seconds)
        return task


@pytest.fixture
def world(tmp_path_factory, monkeypatch):
    base = tmp_path_factory.mktemp("codex-run")
    root = base / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        for name in ("repo", "bin", "codex-home", "tmp"):
            (base / name).mkdir()
        subprocess.run(["git", "init", "-q", str(base / "repo")], check=True)
        wrapper = base / "bin" / "codex"
        wrapper.write_text(f'#!/bin/sh\nexec {sys.executable} {FAKE} "$@"\n')
        wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)
        monkeypatch.setenv("PATH", f"{base / 'bin'}{os.pathsep}{os.environ['PATH']}")
        monkeypatch.setenv("CODEX_HOME", str(base / "codex-home"))
        monkeypatch.setenv("TMPDIR", str(base / "tmp"))
        monkeypatch.setenv("FAKE_CODEX_LOG", str(base / "fake.log"))
        monkeypatch.delenv("FAKE_CODEX_SCENARIO", raising=False)
        registry = TaskRegistry(log_dir=root / "tasks", owner=identity.own_identity())
        yield World(base, registry, monkeypatch)


def request(item="call-1", rid=0, **over):
    params = {"threadId": "$thread", "turnId": "$turn", "itemId": item, "startedAtMs": 5, "kind": "command", "command": "/bin/zsh -lc 'touch x'", "cwd": "$cwd",
              "availableDecisions": ["accept", "cancel"]}
    params.update(over)
    return {"id": rid, "method": app.COMMAND_REQUEST, "params": {k: v for k, v in params.items() if v is not ...}}


def item(kind, item_id, **fields):
    return {"item": {"type": kind, "id": item_id, **fields}, "threadId": "$thread", "turnId": "$turn"}


def command_done(item_id="call-1", status="completed", exit_code=0):
    return {"notify": {"method": "item/completed", "params": item("commandExecution", item_id, command="touch x", status=status, exitCode=exit_code, aggregatedOutput="")}}


def agent(text="finished"):
    return {"notify": {"method": "item/completed", "params": item("agentMessage", "msg-1", text=text)}}


def resolved(request_id=0):
    return {"notify": {"method": "serverRequest/resolved", "params": {"threadId": "$thread", "requestId": request_id}}}


HAPPY = [{"request": request(), "await": "a"}, resolved(), command_done(), agent(), {"complete": "completed"}]


# --- the happy paths ----------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_an_approved_command_round_trip_with_every_frame_in_order(world):
    world.scenario(turn=[{"notify": {"method": "item/started", "params": item("commandExecution", "call-1", command="touch x", status="inProgress")}}, *HAPPY])
    task = await world.start(model="gpt-x")
    assert task.live_input is False and task.control_pipe is True and task.caller_approvals
    (row,) = await world.pending(task)
    assert row["action"]["tool_name"] == "commandExecution" and row["action"]["input"]["command"] == "/bin/zsh -lc 'touch x'" and row["may_run_outside_sandbox"] is True
    world.decide(task, row, "approve")
    await world.settled(task)
    assert task.status == "completed" and task.exit_code == 0 and task.session_id == THREAD and task.acc.summary == "finished"
    assert world.methods() == ["initialize", "initialized", "thread/start", "hooks/list", "turn/start"]
    client = world.client()
    assert [f["id"] for f in client if "id" in f and "method" in f] == [1, 2, 3, 4]
    start = next(f for f in client if f.get("method") == "thread/start")["params"]
    ctx = task.acc.stream_state["control_context"]
    assert start["approvalPolicy"] == "on-request" and start["approvalsReviewer"] == "user" and start["sandbox"] == "workspace-write" and start["model"] == "gpt-x"
    assert start["config"] == app.config_map(ctx) and start["cwd"] == os.path.realpath(world.repo)
    turn = next(f for f in client if f.get("method") == "turn/start")["params"]
    assert set(turn) == {"threadId", "input"} and turn["threadId"] == THREAD and turn["input"] == [{"type": "text", "text": "do it"}]
    answer = next(f for f in client if "method" not in f)
    assert answer == {"id": 0, "result": {"decision": "accept"}}
    argv = world.frames()[0]["argv"]
    assert argv == app.argv(ctx)[1:] and world.frames()[0]["codex_home"] == str(world.base / "codex-home")
    (final,) = world.rows(task)
    assert (final["disposition"], final["delivery"], final["delivered_decision"], final["execution_outcome"]) == ("decided", "sent", "approve", "succeeded")
    assert task.input_closed is True and task.markers == ("codex", "app-server")


@pytest.mark.asyncio
async def test_a_rejected_command_is_declined_and_the_run_still_completes(world):
    world.scenario(turn=[{"request": request(), "await": "a"}, resolved(), command_done(status="declined", exit_code=None), agent("not run"), {"complete": "completed"}])
    task = await world.start()
    (row,) = await world.pending(task)
    world.decide(task, row, "reject")
    await world.settled(task)
    assert next(f for f in world.client() if "method" not in f) == {"id": 0, "result": {"decision": "decline"}}
    (final,) = world.rows(task)
    assert (final["disposition"], final["delivered_decision"], final["execution_outcome"]) == ("decided", "reject", "failed") and task.status == "completed"


@pytest.mark.asyncio
async def test_a_recorded_transcript_replays_through_the_whole_adapter(world):
    world.scenario(turn=[{"replay": str(TRANSCRIPTS / "a1-basic.jsonl"), "home": str(world.base)}])
    task = await world.start()
    for expected, decision in (("c2", "approve"), ("c3", "reject")):
        (row,) = [r for r in await world.pending(task) if r["tool_use_id"] == expected]
        world.decide(task, row, decision)
        await asyncio.sleep(0.2)
    await world.settled(task)
    answers = [f for f in world.client() if "method" not in f]
    assert answers == [{"id": 0, "result": {"decision": "accept"}}, {"id": 1, "result": {"decision": "decline"}}]
    rows = {r["tool_use_id"]: r for r in world.rows(task)}
    assert rows["c2"]["execution_outcome"] == "succeeded" and rows["c3"]["execution_outcome"] == "failed" and rows["c2"]["disposition"] == rows["c3"]["disposition"] == "decided"
    assert task.status == "completed" and task.acc.summary == "done"


@pytest.mark.asyncio
async def test_a_resumed_task_starts_a_new_process_with_thread_resume_and_the_same_pins(world):
    world.scenario(turn=[agent("first"), {"complete": "completed"}])
    first = await world.start()
    await world.settled(first)
    world.log.unlink()
    world.scenario(turn=[agent("second"), {"complete": "completed"}])
    second = await world.registry.resume(first, "again")
    await world.settled(second)
    assert second.status == "completed" and second.session_id == THREAD and second.control_pipe
    assert world.methods() == ["initialize", "initialized", "thread/resume", "hooks/list", "turn/start"]
    resume = next(f for f in world.client() if f.get("method") == "thread/resume")["params"]
    assert resume["threadId"] == THREAD and resume["approvalPolicy"] == "on-request" and resume["config"]["projects"]
    assert next(f for f in world.client() if f.get("method") == "turn/start")["params"]["input"][0]["text"] == "again"
    assert second.acc.stream_state["control_context"]["thread_id"] == THREAD


@pytest.mark.asyncio
async def test_a_resume_with_a_policy_mismatch_stops_before_turn_start(world):
    world.scenario(turn=[agent("first"), {"complete": "completed"}])
    first = await world.start()
    await world.settled(first)
    world.log.unlink()
    world.scenario(thread={"approvalPolicy": "never"})
    second = await world.registry.resume(first, "again")
    await world.settled(second)
    assert second.status != "completed" and "turn/start" not in world.methods() and any("approvalPolicy" in n for n in second.bridge_notices)


# --- verification refusals before anything runs ---------------------------------------------------------------

REFUSALS = {
    "version": ({"user_agent_version": "0.162.0"}, "qualified on 0.163.0", ["initialize"]),
    "cli version": ({"thread_cli_version": "0.164.0"}, "cliVersion", ["initialize", "initialized", "thread/start"]),
    "policy": ({"thread": {"approvalPolicy": "never"}}, "approvalPolicy", ["initialize", "initialized", "thread/start"]),
    "reviewer": ({"thread": {"approvalsReviewer": "auto_review"}}, "approvalsReviewer", ["initialize", "initialized", "thread/start"]),
    "sandbox": ({"thread": {"sandbox": {"type": "dangerFullAccess"}}}, "sandbox type", ["initialize", "initialized", "thread/start"]),
    "writable roots": ({"thread": {"sandbox": {"writableRoots": ["/x"]}}}, "writableRoots", ["initialize", "initialized", "thread/start"]),
    "network": ({"thread": {"sandbox": {"networkAccess": True}}}, "networkAccess", ["initialize", "initialized", "thread/start"]),
    "workspace roots": ({"thread": {"runtimeWorkspaceRoots": ["/x"]}}, "runtimeWorkspaceRoots", ["initialize", "initialized", "thread/start"]),
    "hooks": ({"hooks": [{"key": "k"}]}, "hooks/list", ["initialize", "initialized", "thread/start", "hooks/list"]),
    "hooks malformed": ({"hooks_result": {"data": "x"}}, "hooks/list", ["initialize", "initialized", "thread/start", "hooks/list"]),
    "thread error": ({"errors": {"thread/start": {"code": -32000, "message": "boom"}}}, "error for thread/start", ["initialize", "initialized", "thread/start"]),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(REFUSALS))
async def test_verification_failures_stop_the_run_before_turn_start(world, name):
    scenario, notice, methods = REFUSALS[name]
    world.scenario(turn=[agent(), {"complete": "completed"}], **scenario)
    task = await world.start()
    await world.settled(task)
    assert task.status in {"failed", "cancelled"} and task.status != "completed"
    assert world.methods() == methods and "turn/start" not in world.methods()
    assert any(notice in n for n in task.bridge_notices), task.bridge_notices
    assert world.rows(task) == []


# --- server requests --------------------------------------------------------------------------------------------

OTHER_REQUESTS = [
    ("item/permissions/requestApproval", {"threadId": "$thread", "turnId": "$turn", "itemId": "p1", "permissions": {}}),
    ("mcpServer/elicitation/request", {"threadId": "$thread", "turnId": "$turn", "serverName": "s", "message": "allow?"}),
    ("item/tool/requestUserInput", {"threadId": "$thread", "turnId": "$turn", "itemId": "u1", "questions": [], "isBlocking": True}),
    ("item/tool/call", {"threadId": "$thread", "turnId": "$turn", "tool": "t"}),
    ("applyPatchApproval", {"conversationId": "c"}),
    ("some/unknown/request", {}),
]


@pytest.mark.asyncio
async def test_every_other_server_request_is_answered_and_audited_never_left_waiting(world):
    steps = []
    for number, (method, params) in enumerate(OTHER_REQUESTS):
        wire_id = f"id-{number}" if number % 2 else number
        steps.append({"request": {"id": wire_id, "method": method, "params": params}, "await": f"r{number}"})
    steps += [{"request": {"id": 40, "method": app.FILE_CHANGE_REQUEST, "params": {"threadId": "$thread", "turnId": "$turn", "itemId": "fc1", "grantRoot": None, "reason": None}}, "await": "fc"},
              agent(), {"complete": "completed"}]
    world.scenario(turn=steps)
    task = await world.start()
    await world.settled(task)
    answers = [f for f in world.client() if "method" not in f]
    assert len(answers) == len(OTHER_REQUESTS) + 1 and task.status == "completed"
    for number, answer in enumerate(answers[:-1]):
        assert answer["id"] == (f"id-{number}" if number % 2 else number) and type(answer["id"]) is (str if number % 2 else int)
        assert answer["error"]["code"] == -32000 and "result" not in answer
    assert answers[-1] == {"id": 40, "result": {"decision": "decline"}}
    rows = world.rows(task)
    assert len(rows) == len(OTHER_REQUESTS) + 1 and all(r["disposition"] == "system_rejected" and r["decided_by"] == "harflow" and r["decision"] is None for r in rows)
    assert {r["system_rejection"] for r in rows} == {"unsupported_request", "file_change_unsupported"}


@pytest.mark.asyncio
async def test_qualification_failures_in_a_request_are_declined_not_shown_to_the_caller(world):
    steps = [{"request": request("ws-1", 0, kind="writeStdin", approvalId="w1", availableDecisions=["accept", "cancel"]), "await": "a"},
             {"request": request("old-1", 1, turnId="historic"), "await": "b"}, {"request": request("other-1", 2, threadId="other"), "await": "c"},
             {"request": request("null-1", 3, command=None), "await": "d"}, {"request": request("perm-1", 4, additionalPermissions={"fileSystem": {"write": ["/x"]}}), "await": "e"},
             {"request": request("net-1", 5, networkApprovalContext={"host": "x"}), "await": "f"}, agent(), {"complete": "completed"}]
    world.scenario(turn=steps)
    task = await world.start()
    await world.settled(task)
    assert [a["result"]["decision"] for a in world.client() if "method" not in a] == ["decline"] * 6
    reasons = {r["tool_use_id"]: r["system_rejection"] for r in world.rows(task)}
    assert reasons == {"ws-1": "unsupported_kind", "old-1": "not_active_turn", "other-1": "not_active_turn", "null-1": "missing_command", "perm-1": "additional_permissions", "net-1": "network_context"}
    assert task.pump.done() and task.status == "completed"


@pytest.mark.asyncio
async def test_a_request_without_accept_and_cancel_stops_the_run(world):
    world.scenario(turn=[{"request": request(availableDecisions=["decline"])}, {"hold": True}])
    task = await world.start()
    await world.settled(task)
    assert task.status != "completed" and any("accept and cancel" in n for n in task.bridge_notices)
    assert [f for f in world.client() if "method" not in f] == []


@pytest.mark.asyncio
async def test_a_string_request_id_is_answered_with_the_same_type(world):
    world.scenario(turn=[{"request": request(rid="req-α"), "await": "a"}, agent(), {"complete": "completed"}])
    task = await world.start()
    (row,) = await world.pending(task)
    assert row["native_request_id"] == "s:req-α"
    world.decide(task, row, "approve")
    await world.settled(task)
    assert next(f for f in world.client() if "method" not in f) == {"id": "req-α", "result": {"decision": "accept"}}


@pytest.mark.asyncio
async def test_a_second_request_for_an_item_stops_the_run_after_the_first_was_accepted(world):
    world.scenario(turn=[{"request": request("call-1", 0), "await": "a"}, {"request": request("call-1", 1, kind="writeStdin", approvalId="w1")}, {"hold": True}])
    task = await world.start()
    (row,) = await world.pending(task)
    world.decide(task, row, "approve")
    await world.settled(task)
    rows = world.rows(task)
    first = next(r for r in rows if r["native_request_id"] == "i:0")
    assert (first["disposition"], first["delivery"], first["delivered_decision"], first["execution_outcome"]) == ("decided", "sent", "approve", "unknown")
    audit = [r for r in rows if r["native_request_id"] == "i:1"]
    assert len(audit) == 1 and audit[0]["tool_use_id"] == "unsupported" and audit[0]["disposition"] == "unsupported" and len(rows) == 2
    assert task.status != "completed" and any("Unsupported" in n for n in task.bridge_notices)
    assert [f for f in world.client() if "method" not in f] == [{"id": 0, "result": {"decision": "accept"}}]


@pytest.mark.asyncio
async def test_a_second_request_after_a_rejection_also_stops_the_run_and_a_later_completion_is_not_attributed(world):
    world.scenario(turn=[{"request": request("call-1", 0), "await": "a"}, {"request": request("call-1", 1, kind="writeStdin", approvalId="w1")}, command_done(), {"hold": True}])
    task = await world.start()
    (row,) = await world.pending(task)
    world.decide(task, row, "reject")
    await world.settled(task)
    rows = world.rows(task)
    first = next(r for r in rows if r["native_request_id"] == "i:0")
    assert first["delivered_decision"] == "reject" and first["execution_outcome"] == "unknown"
    assert task.status != "completed" and len(rows) == 2


@pytest.mark.asyncio
async def test_a_server_side_resolution_before_the_decision_closes_eligibility(world):
    world.scenario(turn=[{"request": request()}, resolved(), {"sleep": 0.5}, agent(), {"complete": "completed"}])
    task = await world.start()
    end = time.monotonic() + 10
    while time.monotonic() < end and not any(r["disposition"] == "invalidated" for r in world.rows(task)):
        await asyncio.sleep(0.05)
    (row,) = world.rows(task)
    assert row["disposition"] == "invalidated"
    with pytest.raises(ValueError, match="expired, settled or already answered"):
        approvals.reserve(world.registry.log_dir, task.task_id, row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    await world.settled(task)
    assert [f for f in world.client() if "method" not in f] == [] and (world.rows(task)[0]["execution_outcome"] == "unknown")


@pytest.mark.asyncio
async def test_a_stale_revision_or_digest_cannot_decide_and_an_unanswered_request_expires_as_a_decline(world, monkeypatch):
    monkeypatch.setattr(approvals, "REQUEST_SECONDS", 1)
    world.scenario(turn=[{"request": request(), "await": "a"}, command_done(status="declined", exit_code=None), agent(), {"complete": "completed"}])
    task = await world.start()
    (row,) = await world.pending(task)
    for bad in ({"owner_revision": row["owner_revision"] + 1}, {"action_digest": "0" * 64}, {"generation": "other"}):
        arguments = dict(owner_revision=row["owner_revision"], action_digest=row["action_digest"], generation=row["generation"]) | bad
        with pytest.raises(ValueError, match="Stale"):
            approvals.reserve(world.registry.log_dir, task.task_id, row["request_id"], arguments["action_digest"], arguments["owner_revision"], "approve", arguments["generation"])
    await world.settled(task)
    (final,) = world.rows(task)
    assert final["disposition"] == "expired" and final["expiry_rejection"] is True and final["decision"] is None
    assert next(f for f in world.client() if "method" not in f) == {"id": 0, "result": {"decision": "decline"}}


# --- tripwires ------------------------------------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["hook/started", "hook/completed", "item/autoApprovalReview/started", "item/autoApprovalReview/completed", "guardianWarning", "autoApprovalReview/strictReviewRequired"])
async def test_a_tripwire_notification_stops_the_run_and_closes_the_held_request(world, method):
    world.scenario(turn=[{"request": request()}, {"notify": {"method": method, "params": {"threadId": "$thread"}}}, {"hold": True}])
    task = await world.start()
    await world.settled(task)
    assert task.status != "completed" and any(method in n for n in task.bridge_notices)
    rows = world.rows(task)
    assert [r["action"]["input"].get("reason") for r in rows if r["tool_use_id"] == "unsupported"] == [f"tripwire {method}"]
    (row,) = [r for r in rows if r["tool_use_id"] != "unsupported"]
    assert row["disposition"] in {"invalidated", "cancelled"} and row["delivery"] in {"failed", "delivery_unknown"} and row["execution_outcome"] == "unknown"
    assert [f for f in world.client() if "method" not in f] == []


# --- terminal evidence ------------------------------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("name,steps", [
    ("failed turn after a message", [agent("looks done"), {"complete": "failed", "error": {"message": "stream died"}}]),
    ("interrupted turn", [agent("partial"), {"complete": "interrupted"}]),
    ("clean exit without a terminal turn", [agent("said something"), {"exit": 0}]),
])
async def test_only_a_completed_turn_can_succeed(world, name, steps):
    world.scenario(turn=steps)
    task = await world.start()
    await world.settled(task)
    assert task.status == "failed", name


# --- crashes, interrupts and cleanup -----------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_a_crash_while_a_request_is_held_invalidates_it_and_nothing_is_answered(world):
    world.scenario(turn=[{"request": request()}, {"sleep": 0.4}, {"exit": 7}])
    task = await world.start()
    await world.pending(task)
    await world.settled(task)
    (row,) = world.rows(task)
    assert row["disposition"] in {"invalidated", "cancelled"} and row["delivery"] == "failed" and row["execution_outcome"] == "unknown"
    assert task.status == "failed" and task.exit_code == 7 and [f for f in world.client() if "method" not in f] == []


@pytest.mark.asyncio
async def test_a_crash_after_an_answer_leaves_the_outcome_unknown_and_never_resends(world):
    world.scenario(turn=[{"request": request(), "await": "a"}, {"exit": 9}])
    task = await world.start()
    (row,) = await world.pending(task)
    world.decide(task, row, "approve")
    await world.settled(task)
    (final,) = world.rows(task)
    assert (final["delivery"], final["delivered_decision"], final["execution_outcome"]) == ("sent", "approve", "unknown") and task.status == "failed"
    assert len([f for f in world.client() if "method" not in f]) == 1


@pytest.mark.asyncio
async def test_cancelling_a_held_request_writes_nothing_and_kills_tool_process_groups(world):
    world.scenario(turn=[{"spawn": True}, {"request": request()}, {"hold": True}])
    task = await world.start()
    await world.pending(task)
    spawned = next(f for f in world.frames() if "spawned" in f)
    assert spawned["pgid"] != task.pgid
    await world.registry.cancel(task)
    await world.settled(task)
    assert task.status == "cancelled" and task.input_closed
    (row,) = world.rows(task)
    assert row["disposition"] in {"cancelled", "invalidated"} and row["delivered_decision"] is None
    assert [f for f in world.client() if "method" not in f] == []
    deadline = time.monotonic() + 5
    alive = True
    while alive and time.monotonic() < deadline:
        try:
            os.kill(spawned["spawned"], 0)
            await asyncio.sleep(0.1)
        except ProcessLookupError:
            alive = False
        else:
            if subprocess.run(["ps", "-o", "stat=", "-p", str(spawned["spawned"])], capture_output=True, text=True).stdout.strip().startswith("Z"):
                alive = False
    assert not alive, "the tool process group survived the stop"


@pytest.mark.asyncio
async def test_the_kill_sweep_reaches_tool_groups_when_the_app_server_ignores_sigterm(world):
    world.scenario(turn=[{"spawn": True}, {"request": request()}, {"hold": True}])
    world.monkeypatch.setattr("harflow_mcp.tasks.SIGKILL_GRACE_SECONDS", 0.3)
    task = await world.start()
    await world.pending(task)
    spawned = next(f for f in world.frames() if "spawned" in f)
    os.kill(task.proc.pid, signal.SIGSTOP)   # a stopped process cannot act on SIGTERM, so the escalation path must run
    await world.registry.cancel(task)
    await world.settled(task)
    await asyncio.sleep(0.3)
    result = subprocess.run(["ps", "-o", "stat=", "-p", str(spawned["spawned"])], capture_output=True, text=True).stdout.strip()
    assert result == "" or result.startswith("Z")


# --- the exec path is untouched ------------------------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_without_caller_approvals_codex_still_runs_as_exec(world):
    task = await world.registry.start("plain", world.repo, backend=get("codex"))
    await world.settled(task)
    first = world.frames()[0]["argv"]
    assert first[0] == "exec" and "app-server" not in first and task.control_pipe is False and task.caller_approvals is False


@pytest.mark.asyncio
async def test_pre_spawn_refusals_run_again_for_every_resume_process(world):
    world.scenario(turn=[agent("first"), {"complete": "completed"}])
    first = await world.start()
    await world.settled(first)
    rules = world.base / "codex-home" / "rules"
    rules.mkdir()
    (rules / "default.rules").write_text('prefix_rule(pattern=["touch"], decision="allow")\n')
    from harflow_mcp.backends.base import UnsupportedCapability
    with pytest.raises(UnsupportedCapability, match="allow rule"):
        await world.registry.resume(first, "again")
    (rules / "default.rules").unlink()
    (world.base / "codex-home" / "config.toml").write_text('profile = "x"\n')
    with pytest.raises(UnsupportedCapability, match="profile"):
        await world.registry.resume(first, "again")


@pytest.mark.asyncio
async def test_a_repository_without_a_git_root_is_refused_at_dispatch(world, tmp_path_factory):
    plain = tmp_path_factory.mktemp("plain")
    from harflow_mcp.backends.base import UnsupportedCapability
    with pytest.raises(UnsupportedCapability, match="Git"):
        await world.registry.start("x", plain, backend=get("codex"), caller_approvals=True)
    assert world.frames() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("freedom", ["read_only", "unrestricted"])
async def test_unqualified_freedoms_are_refused_at_dispatch(world, freedom):
    from harflow_mcp.backends.base import UnsupportedCapability
    with pytest.raises(UnsupportedCapability):
        await world.start(freedom=freedom)
    assert world.frames() == []


# --- review round 1 ------------------------------------------------------------------------------------------------------

def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return not subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True).stdout.strip().startswith("Z")


async def gone(pid, seconds=5):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if not alive(pid):
            return True
        await asyncio.sleep(0.1)
    return False


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [True, "ignore_term"])
async def test_a_server_that_outlives_its_completed_turn_is_shut_down_and_the_outcome_stands(world, monkeypatch, mode):
    monkeypatch.setattr("harflow_mcp.tasks.CONTROL_EXIT_GRACE_SECONDS", 0.3)
    monkeypatch.setattr("harflow_mcp.tasks.SIGKILL_GRACE_SECONDS", 0.5)
    world.scenario(turn=[{"spawn": True}, agent("all done"), {"complete": "completed"}, {"linger": mode}])
    task = await world.start()
    await world.settled(task, 40)
    spawned = next(f for f in world.frames() if "spawned" in f)
    assert task.status == "completed" and task.acc.summary == "all done" and not task.cancel_requested
    assert task.exit_code != 0, "Harflow, not the server, ended it"
    assert await gone(spawned["spawned"]), "the surviving tool group was swept"


@pytest.mark.asyncio
async def test_a_tripwire_after_an_answer_leaves_the_outcome_unknown_and_writes_an_audit_entry(world):
    world.scenario(turn=[{"request": request(), "await": "a"}, {"notify": {"method": "hook/started", "params": {"threadId": "$thread"}}}, command_done(), {"hold": True}])
    task = await world.start()
    (row,) = await world.pending(task)
    world.decide(task, row, "approve")
    await world.settled(task)
    rows = world.rows(task)
    answered = next(r for r in rows if r["native_request_id"] == "i:0")
    assert answered["delivered_decision"] == "approve" and answered["execution_outcome"] == "unknown", "a buffered completion is not attributed after a tripwire"
    audit = [r for r in rows if r["tool_use_id"] == "unsupported"]
    assert len(audit) == 1 and audit[0]["disposition"] == "unsupported" and audit[0]["action"]["input"]["reason"] == "tripwire hook/started" and audit[0]["delivery"] == "failed"
    assert task.status != "completed" and any("hook/started" in n for n in task.bridge_notices)


@pytest.mark.asyncio
async def test_model_effort_and_a_large_prompt_reach_only_the_allowed_fields(world):
    big = "p" * (40 * 1024)
    world.scenario(turn=[agent(), {"complete": "completed"}])
    task = await world.start(big, model="gpt-x", reasoning_effort="high", network=True)
    await world.settled(task)
    client = world.client()
    turn = next(f for f in client if f.get("method") == "turn/start")["params"]
    assert set(turn) == {"threadId", "input"} and turn["input"] == [{"type": "text", "text": big}]
    thread = next(f for f in client if f.get("method") == "thread/start")["params"]
    assert thread["model"] == "gpt-x" and thread["config"]["model_reasoning_effort"] == "high" and thread["config"]["sandbox_workspace_write"]["network_access"] is True
    argv = world.frames()[0]["argv"]
    assert 'model_reasoning_effort="high"' in argv and "sandbox_workspace_write.network_access=true" in argv and big not in " ".join(argv)
    assert task.status == "completed" and "HARFLOW_TASK_SCRATCH" not in os.environ


@pytest.mark.asyncio
async def test_a_request_after_the_turn_ended_stops_the_run(world):
    world.scenario(turn=[agent(), {"complete": "completed"}, {"request": request("late-1", 3)}, {"hold": True}])
    task = await world.start()
    await world.settled(task)
    assert task.status in {"completed", "failed", "cancelled"}
    late = [r for r in world.rows(task) if r["native_request_id"] == "i:3"]
    assert late == [] or late[0]["disposition"] in {"unsupported", "system_rejected", "invalidated"}


def test_the_preview_and_launch_notices_say_there_is_no_scratch_directory():
    backend = get("codex")
    assert "HARFLOW_TASK_SCRATCH is not set" in backend.approval_notice(False) and backend.approval_scratch() is False
    assert "Dispatch can still refuse" in backend.approval_limitations(False)


# --- the SIGKILL sweep, on canned process tables --------------------------------------------------------------------------

class Sweep:
    def __init__(self, monkeypatch, tables):
        from harflow_mcp import tasks
        self.tasks, self.tables, self.calls = tasks, list(tables), []
        monkeypatch.setattr(tasks, "_process_table", lambda timeout=5.0: self.tables.pop(0) if len(self.tables) > 1 else self.tables[0])
        monkeypatch.setattr(os, "kill", lambda pid, sig: self.calls.append(("kill", pid, sig)))
        monkeypatch.setattr(os, "killpg", lambda pgid, sig: self.calls.append(("killpg", pgid, sig)))

    def task(self, pid, pgid, returncode=None, tool_procs=None):
        return SimpleNamespace(proc=SimpleNamespace(pid=pid, returncode=returncode), pgid=pgid, task_id="t", tool_procs=dict(tool_procs or {}))

    def run(self, task):
        self.tasks._kill_control_tree(task)
        return self.calls


def row(ppid, pgid, start="s"):
    return (ppid, pgid, start)


def test_the_sweep_stops_the_native_process_when_it_is_the_root(monkeypatch):
    table = {100: row(1, 100), 200: row(100, 200), 201: row(200, 200)}
    sweep = Sweep(monkeypatch, [table])
    calls = sweep.run(sweep.task(100, 100))
    assert calls[0] == ("kill", 100, signal.SIGSTOP)
    assert ("killpg", 200, signal.SIGKILL) in calls and ("killpg", 100, signal.SIGKILL) in calls
    assert calls.index(("killpg", 200, signal.SIGKILL)) < calls.index(("killpg", 100, signal.SIGKILL))


def test_the_sweep_stops_the_native_child_of_a_wrapper_too(monkeypatch):
    table = {100: row(1, 100), 101: row(100, 100), 200: row(101, 200)}
    sweep = Sweep(monkeypatch, [table])
    calls = sweep.run(sweep.task(100, 100))
    stopped = {pid for kind, pid, sig in calls if kind == "kill" and sig == signal.SIGSTOP}
    assert stopped == {100, 101} and ("killpg", 200, signal.SIGKILL) in calls


def test_a_group_created_during_the_cleanup_is_caught_by_the_second_enumeration(monkeypatch):
    first = {100: row(1, 100), 200: row(100, 200)}
    second = {**first, 300: row(200, 300)}
    sweep = Sweep(monkeypatch, [first, first, second, second])
    calls = sweep.run(sweep.task(100, 100))
    assert ("killpg", 200, signal.SIGKILL) in calls and ("killpg", 300, signal.SIGKILL) in calls


def test_members_found_in_a_killed_group_after_the_leader_group_died_are_swept(monkeypatch):
    before = {100: row(1, 100), 200: row(100, 200)}
    after = {200: row(1, 200), 201: row(1, 200)}
    sweep = Sweep(monkeypatch, [before, before, before, after])
    calls = sweep.run(sweep.task(100, 100))
    assert calls.count(("killpg", 200, signal.SIGKILL)) == 2, "the verified group is signalled again, which reaches the straggler"


def test_a_reaped_leader_is_never_walked_and_only_matching_recorded_groups_are_swept(monkeypatch):
    reused = {50: row(1, 50, "a different process")}
    sweep = Sweep(monkeypatch, [reused])
    calls = sweep.run(sweep.task(100, 100, returncode=0, tool_procs={50: (50, "recorded start")}))
    assert not any(sig == signal.SIGSTOP for _k, _p, sig in calls) and ("killpg", 50, signal.SIGKILL) not in calls
    same = Sweep(monkeypatch, [{50: row(1, 50, "recorded start")}])
    assert ("killpg", 50, signal.SIGKILL) in same.run(same.task(100, 100, returncode=0, tool_procs={50: (50, "recorded start")}))
    gone_ = Sweep(monkeypatch, [{}])
    assert ("killpg", 50, signal.SIGKILL) not in gone_.run(gone_.task(100, 100, returncode=0, tool_procs={50: (50, "recorded start")}))


def test_recording_tool_procs_keeps_pid_group_and_start_time(monkeypatch):
    from harflow_mcp import tasks
    table = {100: row(1, 100, "t0"), 200: row(100, 200, "t1"), 201: row(200, 201, "t2")}
    monkeypatch.setattr(tasks, "_process_table", lambda timeout=5.0: table)
    task = SimpleNamespace(proc=SimpleNamespace(pid=100, returncode=None), pgid=100)
    assert tasks._tool_procs(task) == {200: (200, "t1"), 201: (201, "t2")}
    task.proc.returncode = 0
    assert tasks._tool_procs(task) == {}


# --- review round 2 ---------------------------------------------------------------------------------------------------------

def test_a_live_leader_never_signals_a_recorded_group_that_now_belongs_to_another_process(monkeypatch):
    table = {100: row(1, 100, "root"), 200: row(100, 200, "live-tool"), 50: row(1, 50, "an unrelated process")}
    sweep = Sweep(monkeypatch, [table])
    calls = sweep.run(sweep.task(100, 100, tool_procs={50: (50, "the recorded tool"), 200: (200, "live-tool")}))
    assert ("killpg", 50, signal.SIGKILL) not in calls and ("kill", 50, signal.SIGKILL) not in calls
    assert ("killpg", 200, signal.SIGKILL) in calls and ("killpg", 100, signal.SIGKILL) in calls


def test_a_live_leader_signals_a_recorded_group_whose_member_still_matches(monkeypatch):
    table = {100: row(1, 100, "root"), 50: row(1, 50, "the recorded tool")}   # orphaned (ppid 1) but verified by pid, group and start
    sweep = Sweep(monkeypatch, [table])
    calls = sweep.run(sweep.task(100, 100, tool_procs={50: (50, "the recorded tool")}))
    assert ("killpg", 50, signal.SIGKILL) in calls


def test_the_final_sweep_also_requires_a_matching_member(monkeypatch):
    first = {100: row(1, 100, "root"), 200: row(100, 200, "tool")}
    reused_after = {300: row(1, 200, "an unrelated process now in group 200")}
    sweep = Sweep(monkeypatch, [first, first, first, reused_after])
    calls = sweep.run(sweep.task(100, 100))
    assert calls.count(("killpg", 200, signal.SIGKILL)) == 1, "the group was signalled once, while its member was proven; not again for a stranger"


@pytest.mark.asyncio
async def test_a_tool_group_that_survives_a_server_which_exits_normally_is_swept(world):
    world.scenario(turn=[{"spawn": True}, agent("done"), {"complete": "completed"}])
    task = await world.start()
    await world.settled(task, 40)
    spawned = next(f for f in world.frames() if "spawned" in f)
    assert task.status == "completed" and task.exit_code == 0 and not task.acc.stream_state.get("shutdown_after_terminal")
    assert task.tool_procs and spawned["spawned"] in task.tool_procs
    assert await gone(spawned["spawned"]), "the surviving tool group was swept although the server exited by itself"


@pytest.mark.asyncio
async def test_a_cancel_during_the_post_terminal_grace_window_does_not_force_success(world, monkeypatch):
    monkeypatch.setattr("harflow_mcp.tasks.CONTROL_EXIT_GRACE_SECONDS", 30.0)
    world.scenario(turn=[agent("done"), {"complete": "completed"}, {"linger": True}])
    task = await world.start()
    end = time.monotonic() + 20
    while time.monotonic() < end and not (task.input_closed and getattr(task.acc.stream_state.get("app_server"), "terminal", None)):
        await asyncio.sleep(0.05)
    assert task.input_closed and not task.done.is_set()
    await world.registry.cancel(task)
    await world.settled(task)
    assert task.status == "cancelled" and "shutdown_after_terminal" not in task.acc.stream_state
    assert get("codex").classify(task.acc, task.exit_code) == "failed", "the stop was a cancel, so exit code 0 is not forced"
