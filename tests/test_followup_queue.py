"""Queued next-continuation follow-ups (`followups.py`): a durable per-task ledger for tasks without
live input, consumed exactly once by an explicitly started continuation.

No agent CLI runs here: continuations go through a registry whose `_spawn` is a double that writes
the successor's record the way a real spawn does, and fails in whichever way a test needs.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from mcp import MCPError

from polybridge import backends, control, ctl, followups, inbox, retention, server, store
from polybridge.events import events_path
from polybridge.tasks import SessionBusyError, TaskRegistry

from test_registry import make_task

OWNER = {"pid": os.getpid(), "start_time": "Wed Jan  1 00:00:00 2026", "markers": []}
SRC = "src"


def log_dir_of(tmp_path: Path) -> Path:
    return tmp_path / "streams"


def put_record(log_dir: Path, task_id: str = SRC, *, age_days: int = 0, **overrides) -> store.TaskRecord:
    """A settled standalone task with no live input (claude with a turn cap)."""
    when = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    fields = dict(
        task_id=task_id, backend="claude", session_id=f"sess-{task_id}", markers=[f"sess-{task_id}"],
        repo_path=str(log_dir.parent), started_at=when, status="completed", exit_code=0,
        finished_at=when, output_drained=True, prompt="Original assignment", max_turns=3,
    )
    record = store.TaskRecord(**(fields | overrides))
    log_dir.mkdir(parents=True, exist_ok=True)
    assert store.write_landed(log_dir, record)
    return record


def state_doc(log_dir: Path, task_id: str = SRC) -> dict:
    return json.loads(followups.state_path(log_dir, task_id).read_text())


def statuses(log_dir: Path, task_id: str = SRC) -> list[str]:
    return [item["status"] for item in sorted(state_doc(log_dir, task_id)["items"], key=lambda i: i["seq"])]


class Spawns:
    """Replaces `registry._spawn`. Records each call, writes the successor like a real spawn, and
    can fail before the exec (`error` flagged), after registration (`after_register`), or
    ambiguously (`ambiguous`)."""

    def __init__(self, registry: TaskRegistry, monkeypatch: pytest.MonkeyPatch) -> None:
        self.registry = registry
        self.calls: list[dict] = []
        self.invocations: list = []
        self.pre_exec: BaseException | None = None
        self.ambiguous: BaseException | None = None
        self.after_register: BaseException | None = None
        self.during = None  # async callable run while the reservation is held
        self.finished = False
        monkeypatch.setattr(registry, "_spawn", self)

    async def __call__(self, invocation, **kwargs):
        self.calls.append(kwargs)
        self.invocations.append(invocation)
        if self.during is not None:
            await self.during()
        if self.pre_exec is not None:
            self.pre_exec.polybridge_not_started = True
            raise self.pre_exec
        if self.ambiguous is not None:
            raise self.ambiguous
        task = make_task(
            self.registry.log_dir, kwargs["task_id"], session_id=kwargs["session_id"], finished=self.finished
        )
        task.prompt = kwargs["prompt"]
        task.parent_task_id = kwargs.get("parent_task_id")
        task.continuation_of_task_id = kwargs.get("continuation_of_task_id")
        self.registry._tasks[task.task_id] = task
        self.registry.persist(task)
        if self.after_register is not None:
            raise self.after_register
        return task

    @property
    def prompts(self) -> list[str]:
        return [call["prompt"] for call in self.calls]


@pytest.fixture(autouse=True)
def clean_process_state():
    """The module keeps in-flight attempts, owed terminal writes and a successor cache per process."""
    for state in (followups._INFLIGHT, followups._PENDING, followups._SUCCESSOR_CACHE):
        state.clear()
    yield
    for state in (followups._INFLIGHT, followups._PENDING, followups._SUCCESSOR_CACHE):
        state.clear()


@pytest.fixture
def registry(tmp_path: Path) -> TaskRegistry:
    registry = server._reg()
    registry._owner = OWNER
    return registry


@pytest.fixture
def spawns(registry: TaskRegistry, monkeypatch: pytest.MonkeyPatch) -> Spawns:
    return Spawns(registry, monkeypatch)


@pytest.fixture
def log_dir(registry: TaskRegistry) -> Path:
    return registry.log_dir


def send(log_dir: Path, text: str = "hello", task_id: str = SRC, **kwargs) -> dict:
    return followups.send(log_dir, task_id, text, by=None, **kwargs)


# --- sending -------------------------------------------------------------------------------------

async def test_a_task_without_live_input_queues_instead_of_refusing(log_dir: Path) -> None:
    put_record(log_dir)
    result = await server.send_message(SRC, "  keep  my   spacing \r\n")
    assert result["status"] == "queued" and result["delivery"] == "next_continuation"
    assert result["seq"] == 1 and "nothing was started" in result["note"]
    assert "delivered" not in result["status"]
    item = state_doc(log_dir)["items"][0]
    assert item["text"] == "  keep  my   spacing \r\n" and item["status"] == "queued"
    assert item["id"] == result["message_id"]
    # A separate store: the live-input inbox is never touched.
    assert not inbox.inbox_path(log_dir, SRC).exists()
    assert not inbox.closed_path(log_dir, SRC).exists()


async def test_a_live_input_task_keeps_its_inbox_path(log_dir: Path, identities) -> None:
    put_record(log_dir, live_input=True, status="running", exit_code=None, finished_at=None,
               owner={"pid": 424_242, "start_time": "Wed Jan  1 00:00:00 2026", "markers": []})
    result = await server.send_message(SRC, "to the running agent")
    assert result["status"] == "queued" and result["delivery"] == "live_input"
    assert not followups.state_path(log_dir, SRC).exists()
    assert [m["text"] for m in inbox.read_new(log_dir, SRC, 0)[0]] == ["to the running agent"]


async def test_a_closed_live_task_is_not_redirected_into_the_ledger(log_dir: Path, identities) -> None:
    put_record(log_dir, live_input=True, status="running", exit_code=None, finished_at=None,
               owner={"pid": 424_242, "start_time": "Wed Jan  1 00:00:00 2026", "markers": []})
    inbox.mark_closed(log_dir, SRC)
    with pytest.raises(MCPError, match="finished; continue with resume_task"):
        await server.send_message(SRC, "too late")
    assert not followups.state_path(log_dir, SRC).exists()


def test_unknown_and_workflow_owned_tasks_are_refused(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir, task_id="nope")
    assert caught.value.code == "unknown_task"
    put_record(log_dir, workflow_builder=True)
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir)
    assert caught.value.code == "workflow_owned"
    put_record(log_dir, "owned")
    from polybridge import workflow_hooks

    def owned(*args, **kwargs):
        raise inbox.SendRefused("This task belongs to an active workflow", code="workflow_active")

    monkeypatch.setattr(workflow_hooks, "refuse_direct_message", owned)
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir, task_id="owned")
    assert caught.value.code == "workflow_active"
    with pytest.raises(inbox.SendRefused) as caught:
        followups.cancel(log_dir, "owned", "x")
    assert caught.value.code == "workflow_active"


def test_size_and_count_caps(log_dir: Path) -> None:
    put_record(log_dir)
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir, "x" * (followups.MAX_MESSAGE_BYTES + 1))
    assert caught.value.code == "message_too_large"
    # Counted in UTF-8 bytes, not characters.
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir, "é" * (followups.MAX_MESSAGE_BYTES // 2 + 1))
    assert caught.value.code == "message_too_large"
    exact = "y" * followups.MAX_MESSAGE_BYTES
    for _ in range(followups.MAX_OUTSTANDING):
        send(log_dir, exact)
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir, "one more")
    assert caught.value.code == "queue_full"
    assert followups.outstanding_count(log_dir, SRC) == followups.MAX_OUTSTANDING
    assert followups.state_path(log_dir, SRC).stat().st_size <= followups.ADMISSION_BUDGET_BYTES


def test_escape_heavy_messages_are_refused_before_the_document_passes_budget(log_dir: Path) -> None:
    put_record(log_dir)
    heavy = "\x01" * followups.MAX_MESSAGE_BYTES  # six bytes each once JSON-escaped
    accepted = 0
    with pytest.raises(inbox.SendRefused) as caught:
        for _ in range(followups.MAX_OUTSTANDING):
            send(log_dir, heavy)
            accepted += 1
    assert caught.value.code == "queue_full" and 0 < accepted < followups.MAX_OUTSTANDING
    path = followups.state_path(log_dir, SRC)
    assert path.stat().st_size <= followups.ADMISSION_BUDGET_BYTES < followups.READ_BUDGET_BYTES
    assert followups.view(log_dir, SRC)["state"] == "available"
    assert [item["text"] for item in followups.view(log_dir, SRC)["items"]] == [heavy] * accepted


def test_repeated_queue_and_cancel_cycles_stay_within_budget(log_dir: Path) -> None:
    put_record(log_dir)
    big = "z" * followups.MAX_MESSAGE_BYTES
    for _ in range(120):
        followups.cancel(log_dir, SRC, send(log_dir, big)["message_id"])
    doc = state_doc(log_dir)
    assert len(doc["items"]) == followups.MAX_FINAL and doc["omitted_final"] == 100
    assert doc["next_seq"] == 121  # seq is never reused
    assert all("text" not in item for item in doc["items"])
    assert followups.state_path(log_dir, SRC).stat().st_size < 8 * 1024
    view = followups.view(log_dir, SRC)
    assert view["omitted_final"] == 100 and len(view["items"]) == followups.MAX_FINAL


def test_an_unreadable_ledger_reads_unavailable_and_refuses_sends(log_dir: Path) -> None:
    put_record(log_dir)
    send(log_dir)
    path = followups.state_path(log_dir, SRC)
    for damage in (b"{not json", b'{"v":2}', b"x" * (followups.READ_BUDGET_BYTES + 1)):
        path.write_bytes(damage)
        assert followups.view(log_dir, SRC) == {"state": "unavailable", "items": [], "omitted_final": 0}
        with pytest.raises(inbox.SendRefused) as caught:
            send(log_dir)
        assert caught.value.code == "unavailable"
        assert followups.outstanding_count(log_dir, SRC) is None


def test_concurrent_sends_from_threads_get_unique_ordered_seqs(log_dir: Path) -> None:
    put_record(log_dir)
    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda n: send(log_dir, f"message {n}"), range(12)))
    assert sorted(r["seq"] for r in results) == list(range(1, 13))
    assert len({r["message_id"] for r in results}) == 12
    items = sorted(state_doc(log_dir)["items"], key=lambda i: i["seq"])
    assert [i["seq"] for i in items] == list(range(1, 13))
    assert {i["text"] for i in items} == {f"message {n}" for n in range(12)}


def test_concurrent_sends_from_processes_get_unique_ordered_seqs(log_dir: Path) -> None:
    put_record(log_dir)
    code = (
        "import sys\nfrom pathlib import Path\nfrom polybridge import followups\n"
        "for n in range(5):\n"
        "    followups.send(Path(sys.argv[1]), 'src', f'{sys.argv[2]}-{n}', by=None)\n"
    )
    procs = [subprocess.Popen([sys.executable, "-c", code, str(log_dir), name]) for name in "abc"]
    assert [p.wait(timeout=60) for p in procs] == [0, 0, 0]
    items = sorted(state_doc(log_dir)["items"], key=lambda i: i["seq"])
    assert [i["seq"] for i in items] == list(range(1, 16))
    for name in "abc":  # per-sender order survives the interleaving
        mine = [i["text"] for i in items if i["text"].startswith(name)]
        assert mine == [f"{name}-{n}" for n in range(5)]


def test_a_busy_lock_is_reported_and_never_waited_on_forever(log_dir: Path) -> None:
    put_record(log_dir)
    holder = os.open(followups.lock_path(log_dir, SRC), os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        with pytest.raises(inbox.SendRefused) as caught:
            send(log_dir, timeout=0.2)
        assert caught.value.code == "lock_timeout"
    finally:
        os.close(holder)
    assert send(log_dir)["seq"] == 1


# --- cancel ---------------------------------------------------------------------------------------

async def test_cancel_queued_message(log_dir: Path) -> None:
    put_record(log_dir)
    first = await server.send_message(SRC, "first")
    second = await server.send_message(SRC, "second")
    result = await server.cancel_queued_message(SRC, first["message_id"])
    assert result == {"task_id": SRC, "message_id": first["message_id"], "status": "cancelled"}
    assert statuses(log_dir) == ["cancelled", "queued"]
    cancelled = next(i for i in state_doc(log_dir)["items"] if i["id"] == first["message_id"])
    assert "text" not in cancelled  # metadata only
    with pytest.raises(MCPError, match="cancelled, not queued"):
        await server.cancel_queued_message(SRC, first["message_id"])
    with pytest.raises(MCPError, match="no message"):
        await server.cancel_queued_message(SRC, "0" * 32)
    with pytest.raises(MCPError, match="unknown task_id"):
        await server.cancel_queued_message("nope", second["message_id"])


async def test_cancel_is_refused_for_reserved_and_consumed_messages(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    mid = send(log_dir)["message_id"]

    async def during():
        with pytest.raises(inbox.SendRefused) as caught:
            await asyncio.to_thread(followups.cancel, log_dir, SRC, mid)
        assert caught.value.code == "not_cancellable" and "reserved" in str(caught.value)

    spawns.during = during
    await registry.resume_record(record, "go", consume_followups=True)
    with pytest.raises(MCPError, match="consumed, not queued"):
        await server.cancel_queued_message(SRC, mid)


# --- consumption: composition ---------------------------------------------------------------------

async def test_resume_composes_every_queued_message_verbatim_in_order(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    texts = ["  first  with   spaces\r\n\ttab", "second\n", "ünïcode — third"]
    sent = [send(log_dir, text) for text in texts]
    queued_at = {i["id"]: i["queued_at"] for i in state_doc(log_dir)["items"]}
    task = await registry.resume_record(record, "Now do it\n", consume_followups=True)
    blocks = [
        f"Queued caller follow-up {n} of 3 (message {s['message_id']}, queued {queued_at[s['message_id']]}):\n{t}\n"
        for n, (s, t) in enumerate(zip(sent, texts), 1)
    ]
    assert spawns.prompts == ["\n".join(blocks) + "\n" + "Now do it\n"]
    assert task.task_id == spawns.calls[0]["task_id"]
    assert spawns.calls[0]["message_ids"] == [s["message_id"] for s in sent]
    assert statuses(log_dir) == ["consumed"] * 3
    consumed = state_doc(log_dir)["items"]
    assert all(i["successor_task_id"] == task.task_id and "text" not in i for i in consumed)
    assert state_doc(log_dir)["sealed_by"] == task.task_id


async def test_resume_without_an_explicit_prompt_ends_with_the_fixed_closing_line(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    send(log_dir, "only this\n\n")
    await registry.resume_record(record, None, consume_followups=True)
    prompt = spawns.prompts[0]
    assert prompt.endswith("\n" + followups.CLOSING_LINE) and not prompt[-1].isspace()
    assert "only this\n\n\n" in prompt


async def test_followup_prompt_may_be_omitted_only_when_something_is_queued(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    put_record(log_dir)
    with pytest.raises(MCPError, match="no queued follow-ups"):
        await server.resume_task(SRC)
    assert spawns.calls == []
    for blank in ("", "   "):
        with pytest.raises(MCPError, match="non-empty"):
            await server.resume_task(SRC, blank)
    send(log_dir, "queued one")
    task = (await server.resume_task(SRC))["task_id"]
    assert spawns.calls[0]["task_id"] == task and "queued one" in spawns.prompts[0]
    # An explicit prompt with an empty queue is an ordinary resume: untouched prompt, no events.
    put_record(log_dir, "other")
    await server.resume_task("other", "plain  prompt")
    assert spawns.prompts[1] == "plain  prompt" and spawns.calls[1]["message_ids"] is None


async def test_the_successors_task_started_event_lists_the_consumed_ids(
    log_dir: Path, registry: TaskRegistry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real `_spawn`, with a shell for an agent."""
    sink = log_dir.parent / "prompt.txt"

    class Shell:
        name = "followup-shell"
        binary = "/bin/sh"
        capabilities = backends.CodexBackend.capabilities

        def build_resume_argv(self, prompt, **kwargs):
            return backends.Invocation([self.binary, "-c", f'printf %s "$0" > {sink}', prompt])

        def assert_safe(self, invocation, freedom, network=None):
            pass

        def enforcement(self, freedom, network=None):
            return backends.Enforcement(freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False)

        def ingest(self, event, acc):
            pass

        def normalize(self, event, acc):
            return []

        def classify(self, acc, exit_code):
            return "completed" if exit_code == 0 else "failed"

    backend = Shell()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    record = put_record(log_dir, backend=backend.name)
    ids = [send(log_dir, "a")["message_id"], send(log_dir, "b")["message_id"]]
    task = await registry.resume_record(record, "go", consume_followups=True)
    await task.done.wait()
    started = json.loads(events_path(log_dir, task.task_id).read_text().splitlines()[0])
    assert started["kind"] == "task_started" and started["message_ids"] == ids
    assert sink.read_text() == task.prompt and "Queued caller follow-up 2 of 2" in task.prompt
    assert statuses(log_dir) == ["consumed", "consumed"]


@pytest.mark.parametrize("backend_name", ["codex", "opencode", "vibe"])
def test_a_composition_over_32_kib_is_carried_whole_by_one_shot_stdin_backends(backend_name: str) -> None:
    backend = backends.get(backend_name)
    items = [{"id": f"{n:032x}", "queued_at": "2026-01-01T00:00:00+00:00", "text": "w" * 20_000} for n in range(3)]
    explicit = "Finish up."
    for prompt in (followups.compose(items, explicit), followups.compose(items, None)):
        assert len(prompt.encode()) > 32 * 1024 and prompt == prompt.strip()
        invocation = backend.build_resume_argv(
            prompt, repo=Path("/tmp"), freedom="write_in_repo", session_id="s1",
            model=None, max_turns=None, reasoning_effort=None, network=None,
        )
        assert invocation.stdin_mode == "pipe_once"
        assert invocation.initial_input == prompt.encode()
        assert prompt not in "".join(invocation.argv)


# --- consumption: failures and the sequence ---------------------------------------------------------

async def test_a_send_during_an_attempt_is_superseded_and_accepted_again_after_a_release(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    send(log_dir, "kept")
    refused: list[inbox.SendRefused] = []

    async def during():
        try:
            await asyncio.to_thread(send, log_dir, "too late")
        except inbox.SendRefused as exc:
            refused.append(exc)

    spawns.during = during
    spawns.pre_exec = OSError("no executable")
    with pytest.raises(OSError):
        await registry.resume_record(record, "go", consume_followups=True)
    assert [e.code for e in refused] == ["superseded"]
    assert spawns.calls[0]["task_id"] in str(refused[0])
    # Provably not started: released, unsealed, re-sendable, and consumable by a later attempt.
    assert statuses(log_dir) == ["queued"] and state_doc(log_dir)["sealed_by"] is None
    assert send(log_dir, "after release")["seq"] == 2
    spawns.during = spawns.pre_exec = None
    await registry.resume_record(record, "go again", consume_followups=True)
    assert statuses(log_dir) == ["consumed", "consumed"]
    assert spawns.prompts[1].index("kept") < spawns.prompts[1].index("after release")


@pytest.mark.parametrize("failure", ["busy", "unsupported", "build"])
async def test_failures_before_the_exec_release_the_messages(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    record = put_record(log_dir)
    send(log_dir, "survives")
    if failure == "busy":
        sibling = make_task(log_dir, "busy", session_id=record.session_id)
        registry._tasks["busy"] = sibling
        expected = SessionBusyError
    elif failure == "unsupported":
        monkeypatch.setattr(
            backends.ClaudeBackend, "build_resume_argv",
            lambda *a, **k: (_ for _ in ()).throw(backends.UnsupportedCapability("nope")),
        )
        expected = backends.UnsupportedCapability
    else:
        monkeypatch.setattr(
            backends.ClaudeBackend, "build_resume_argv", lambda *a, **k: (_ for _ in ()).throw(ValueError("bad"))
        )
        expected = ValueError
    with pytest.raises(expected) as caught:
        await registry.resume_record(record, "go", consume_followups=True)
    if failure == "build":
        assert caught.value.polybridge_not_started is True
    assert spawns.calls == []
    assert statuses(log_dir) == ["queued"] and state_doc(log_dir)["sealed_by"] is None
    assert send(log_dir, "more")["seq"] == 2


@pytest.mark.parametrize("error", [RuntimeError("boom"), asyncio.CancelledError()], ids=["exception", "cancelled"])
async def test_an_ambiguous_spawn_failure_leaves_delivery_unknown(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns, error: BaseException
) -> None:
    record = put_record(log_dir)
    send(log_dir, "maybe delivered")
    spawns.ambiguous = error
    with pytest.raises(type(error)):
        await registry.resume_record(record, "go", consume_followups=True)
    assert statuses(log_dir) == ["delivery_unknown"]
    item = state_doc(log_dir)["items"][0]
    assert item["text"] == "maybe delivered" and "inspect before resending" in item["reason"]
    # Never re-queued, never silently dropped, and the source is sealed.
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir)
    assert caught.value.code == "superseded"
    spawns.ambiguous = None
    await registry.resume_record(record, "explicit only", consume_followups=True)
    assert "maybe delivered" not in spawns.prompts[-1]
    view = followups.view(log_dir, SRC)
    assert [(i["status"], i["text"]) for i in view["items"]] == [("delivery_unknown", "maybe delivered")]


async def test_an_exception_after_the_successor_registered_reads_consumed(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    send(log_dir, "reached it")
    spawns.after_register = RuntimeError("flush failed")
    with pytest.raises(RuntimeError):
        await registry.resume_record(record, "go", consume_followups=True)
    assert statuses(log_dir) == ["consumed"]


async def test_a_terminal_write_failure_never_changes_a_successful_launch(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = put_record(log_dir)
    send(log_dir, "delivered")
    real = followups._locked
    broken = [False]

    def locked(*args, **kwargs):
        if broken[0]:
            raise control.LockTimeout("busy")
        return real(*args, **kwargs)

    monkeypatch.setattr(followups, "_locked", locked)

    async def during():
        broken[0] = True  # the reservation is in; every later lock attempt fails

    spawns.during = during
    task = await registry.resume_record(record, "go", consume_followups=True)
    assert task.task_id == spawns.calls[0]["task_id"]
    assert any("marking them consumed failed" in n for n in task.bridge_notices)
    assert statuses(log_dir) == ["reserved"]  # nothing landed ...
    assert followups.view(log_dir, SRC)["items"][0]["status"] == "consumed"  # ... but the read is truthful
    broken[0] = False
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir)  # taking the lock repairs the ledger first
    assert caught.value.code == "superseded"
    assert statuses(log_dir) == ["consumed"]


async def test_a_failed_release_never_masks_the_original_error_and_is_retried(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = put_record(log_dir)
    send(log_dir, "still ours")
    real = followups._locked
    broken = [False]

    def locked(*args, **kwargs):
        if broken[0]:
            raise control.LockTimeout("busy")
        return real(*args, **kwargs)

    monkeypatch.setattr(followups, "_locked", locked)

    async def during():
        broken[0] = True

    spawns.during = during
    spawns.pre_exec = OSError("no executable")
    with pytest.raises(OSError, match="no executable"):
        await registry.resume_record(record, "go", consume_followups=True)
    broken[0] = False
    # A status read in between reports the release this process still owes, not a lost message.
    assert [i["status"] for i in followups.view(log_dir, SRC)["items"]] == ["queued"]
    # The pessimistic read in between never wins: the next lock holder applies the release first.
    assert send(log_dir, "after")["seq"] == 2
    assert statuses(log_dir) == ["queued", "queued"] and state_doc(log_dir)["sealed_by"] is None


async def test_resumes_started_together_on_one_session_consume_each_message_once_and_the_rest_are_busy(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    for n in range(3):
        send(log_dir, f"message {n}")
    results = await asyncio.gather(
        *(registry.resume_record(record, f"prompt {n}", consume_followups=True) for n in range(4)),
        return_exceptions=True,
    )
    started = [r for r in results if not isinstance(r, BaseException)]
    assert len(started) == 1 and all(isinstance(r, SessionBusyError) for r in results if r not in started)
    for n in range(3):
        assert sum(p.count(f"\nmessage {n}\n") for p in spawns.prompts) == 1
    assert statuses(log_dir) == ["consumed"] * 3


async def test_a_second_resume_after_the_first_finished_consumes_nothing_twice(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    send(log_dir, "once only")
    spawns.finished = True
    await registry.resume_record(record, "first", consume_followups=True)
    await registry.resume_record(record, "second", consume_followups=True)
    assert "once only" in spawns.prompts[0] and spawns.prompts[1] == "second"
    with pytest.raises(MCPError, match="no queued follow-ups"):
        await server.resume_task(SRC)


async def test_resumes_without_the_flag_never_consume(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    """The workflow supervisor and builder resumes pass a `task_id` and no flag."""
    record = put_record(log_dir)
    send(log_dir, "not for a workflow")
    await registry.resume_record(record, "supervisor prompt", task_id="a" * 32)
    assert spawns.prompts == ["supervisor prompt"] and spawns.calls[0]["message_ids"] is None
    assert statuses(log_dir) == ["queued"] and state_doc(log_dir)["sealed_by"] is None
    parent = make_task(log_dir, "live-parent", session_id="live-sess", finished=True)
    registry._tasks[parent.task_id] = parent
    registry.persist(parent)
    send(log_dir, "also queued", task_id="live-parent")
    await registry.resume(parent, "supervisor prompt 2")
    assert spawns.prompts[1] == "supervisor prompt 2"
    assert followups.outstanding_count(log_dir, "live-parent") == 1
    with pytest.raises(ValueError, match="required"):
        await registry.resume_record(record, None)


async def test_resume_of_a_live_parent_consumes(log_dir: Path, registry: TaskRegistry, spawns: Spawns) -> None:
    parent = make_task(log_dir, "live-parent", session_id="live-sess", finished=True)
    registry._tasks[parent.task_id] = parent
    registry.persist(parent)
    send(log_dir, "for the live parent", task_id="live-parent")
    child = (await server.resume_task("live-parent"))["task_id"]
    assert child == spawns.calls[0]["task_id"] and "for the live parent" in spawns.prompts[0]
    assert statuses(log_dir, "live-parent") == ["consumed"]


async def test_a_sibling_resume_makes_older_queued_messages_undeliverable(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    send(log_dir, "meant for the next one")
    spawns.finished = True
    await registry.resume_record(record, "a sibling took the session", task_id="b" * 32)  # no consumption
    sibling = spawns.calls[0]["task_id"]
    view = followups.view(log_dir, SRC)
    assert [i["status"] for i in view["items"]] == ["undeliverable"]
    assert f"superseded by {sibling}" in view["items"][0]["reason"] and view["items"][0]["text"]
    with pytest.raises(inbox.SendRefused) as caught:
        send(log_dir, "again")
    assert caught.value.code == "superseded" and sibling in str(caught.value)
    assert statuses(log_dir) == ["undeliverable"]  # persisted by the lock holder
    # A later explicit resume of the older checkpoint does not resurrect it.
    await registry.resume_record(record, "older checkpoint", consume_followups=True)
    assert spawns.prompts[-1] == "older checkpoint"


# --- crash leftovers: read-time resolution -------------------------------------------------------------

def reserved_ledger(log_dir: Path, successor: str, owner: dict) -> None:
    put_record(log_dir)
    send(log_dir, "was in flight")
    doc = state_doc(log_dir)
    doc["items"][0].update(status="reserved", successor_task_id=successor, owner=owner)
    doc["sealed_by"] = successor
    followups.state_path(log_dir, SRC).write_text(json.dumps(doc))


def test_a_reserved_item_whose_successor_exists_reads_consumed_and_the_next_lock_holder_persists_it(
    log_dir: Path,
) -> None:
    reserved_ledger(log_dir, "succ", {"pid": 1, "start_time": "x", "markers": []})
    put_record(log_dir, "succ", parent_task_id=SRC)
    assert followups.view(log_dir, SRC)["items"][0]["status"] == "consumed"
    assert statuses(log_dir) == ["reserved"]  # a status read never writes
    with pytest.raises(inbox.SendRefused):
        send(log_dir)  # a lock holder repairs it
    assert statuses(log_dir) == ["consumed"]


def test_a_reserved_item_with_a_dead_owner_and_no_successor_is_delivery_unknown(
    log_dir: Path, identities
) -> None:
    owner = {"pid": 777_777, "start_time": "x", "markers": []}
    identities.dead(777_777)
    reserved_ledger(log_dir, "never-started", owner)
    view = followups.view(log_dir, SRC)
    assert view["items"][0]["status"] == "delivery_unknown" and view["items"][0]["text"] == "was in flight"
    with pytest.raises(inbox.SendRefused):
        send(log_dir)
    assert statuses(log_dir) == ["delivery_unknown"]
    assert state_doc(log_dir)["sealed_by"] == "never-started"


def test_another_live_owners_reservation_stays_reserved(log_dir: Path, identities) -> None:
    identities.alive(777_778)
    reserved_ledger(log_dir, "being-started", {"pid": 777_778, "start_time": "x", "markers": []})
    assert followups.view(log_dir, SRC)["items"][0]["status"] == "reserved"
    assert statuses(log_dir) == ["reserved"]


def test_this_process_reservation_no_longer_in_flight_is_delivery_unknown(log_dir: Path) -> None:
    reserved_ledger(log_dir, "lost-write", {"pid": os.getpid(), "start_time": None, "markers": []})
    assert followups.view(log_dir, SRC)["items"][0]["status"] == "delivery_unknown"
    followups.state_path(log_dir, SRC).unlink()
    followups._INFLIGHT.add((str(log_dir), "in-progress"))
    try:
        reserved_ledger(log_dir, "in-progress", {"pid": os.getpid(), "start_time": None, "markers": []})
        assert followups.view(log_dir, SRC)["items"][0]["status"] == "reserved"
    finally:
        followups._INFLIGHT.discard((str(log_dir), "in-progress"))


def test_the_superseded_scan_is_skipped_without_queued_items(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    put_record(log_dir)
    calls = []
    monkeypatch.setattr(followups, "successor_candidates", lambda *a, **k: calls.append(1) or [])
    assert followups.view(log_dir, SRC)["items"] == []  # no state file at all
    mid = send(log_dir)["message_id"]
    calls.clear()
    followups.cancel(log_dir, SRC, mid)  # resolves once under the lock, with a queued item
    calls.clear()
    followups.view(log_dir, SRC)  # only a final item left
    assert calls == []


# --- status surfaces -------------------------------------------------------------------------------------

async def test_status_carries_queued_followups_live_and_recovered(
    log_dir: Path, registry: TaskRegistry
) -> None:
    task = make_task(log_dir, "live-one", session_id="sess-live", finished=True)
    registry._tasks[task.task_id] = task
    registry.persist(task)
    put_record(log_dir)
    for target in ("live-one", SRC):
        await server.send_message(target, f"for {target}")
        await server.send_message(target, "second")
    assert "queued_followups" not in registry.get("live-one").snapshot()  # never built on the loop
    live = (await server.get_task_status("live-one"))["queued_followups"]
    recovered = store.snapshot(log_dir, store.read(log_dir, SRC))["queued_followups"]
    for view, name in ((live, "live-one"), (recovered, SRC)):
        assert view["state"] == "available" and view["omitted_final"] == 0
        assert [i["text"] for i in view["items"]] == [f"for {name}", "second"]
        assert set(view["items"][0]) == {"id", "seq", "status", "text", "queued_at"}
    assert registry.get("live-one").brief()["queued_followup_count"] == 2
    assert store.brief(log_dir, store.read(log_dir, SRC))["queued_followup_count"] == 2
    payload = await server.get_task_status(SRC)
    assert payload["queued_followups"]["items"][1]["text"] == "second"
    put_record(log_dir, "plain")
    assert store.snapshot(log_dir, store.read(log_dir, "plain"))["queued_followups"] == {
        "state": "available", "items": [], "omitted_final": 0}
    assert store.brief(log_dir, store.read(log_dir, "plain"))["queued_followup_count"] == 0
    assert not followups.state_path(log_dir, "plain").exists()


# --- ctl -----------------------------------------------------------------------------------------------------

@pytest.fixture
def ctl_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    from polybridge.tasks import default_log_dir

    return default_log_dir()


def test_ctl_send_and_the_mcp_tool_share_the_dispatcher(
    ctl_home: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    put_record(ctl_home)
    assert ctl.main(["send", SRC, "from the cli", "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["v"] == 14 and doc["result"]["delivery"] == "next_continuation" and doc["result"]["seq"] == 1
    assert state_doc(ctl_home)["items"][0]["text"] == "from the cli"
    assert ctl.main(["send", SRC, "x" * (followups.MAX_MESSAGE_BYTES + 1), "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "message_too_large"
    assert ctl.main(["send", SRC, "plain text"]) == 0
    assert "next continuation; nothing started or delivered" in capsys.readouterr().out
    put_record(ctl_home, "done")
    followups.send(ctl_home, "done", "x", by=None)
    # The same workflow refusal as the MCP tool, on the ctl path.
    from polybridge import workflow_hooks

    def owned(*args, **kwargs):
        raise inbox.SendRefused("workflow", code="workflow_active")

    monkeypatch.setattr(workflow_hooks, "refuse_direct_message", owned)
    assert ctl.main(["send", SRC, "refused", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "workflow_active"
    assert ctl.main(["send", "nope", "x", "--json"]) == 1


def test_ctl_status_json_carries_the_ledger(ctl_home: Path, capsys: pytest.CaptureFixture) -> None:
    put_record(ctl_home)
    followups.send(ctl_home, SRC, "visible", by=None)
    assert ctl.main(["status", SRC, "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["v"] == 14 and doc["task"]["queued_followups"]["items"][0]["text"] == "visible"


async def test_ctl_resume_consumes_and_its_text_is_optional(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    put_record(log_dir)
    send(log_dir, "via ctl")
    spawns.finished = True
    args = argparse.Namespace(task_id=SRC, text=None, max_turns=None, network=None, json=True)
    task = await ctl._resume_action(args)(registry)
    assert "via ctl" in spawns.prompts[0] and spawns.calls[0]["message_ids"] and statuses(log_dir) == ["consumed"]
    assert task.task_id == spawns.calls[0]["task_id"]
    with pytest.raises(ValueError, match="no queued follow-ups"):
        await ctl._resume_action(args)(registry)
    parser = ctl._build_parser()[0]
    assert parser.parse_args(["resume", SRC]).text is None
    assert parser.parse_args(["resume", SRC, "go"]).text == "go"


# --- continue_task ----------------------------------------------------------------------------------------------

async def test_continue_task_with_an_empty_queue_is_unchanged(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    put_record(log_dir)
    child = await registry.continue_task(SRC, None, None, consume_followups=True)
    from polybridge.task_continuation import context

    assert spawns.prompts[0] == context(registry, store.read(log_dir, SRC), None, None)
    assert not followups.state_path(log_dir, SRC).exists()  # nothing written
    assert (await registry.continue_task(SRC, None, None, consume_followups=True)) is child


async def test_continue_task_consumes_ahead_of_the_extra_prompt(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    put_record(log_dir)
    mid = send(log_dir, "  queued  prose\n")["message_id"]
    child = await registry.continue_task(SRC, "Extra  prompt\n", None, consume_followups=True)
    prompt = spawns.prompts[0]
    assert f"(message {mid}, queued " in prompt and "  queued  prose\n\n" in prompt
    assert prompt.index("queued  prose") < prompt.index("Caller follow-up:\nExtra  prompt\n")
    assert spawns.calls[0]["message_ids"] == [mid]
    assert statuses(log_dir) == ["consumed"] and state_doc(log_dir)["items"][0]["successor_task_id"] == child.task_id
    put_record(log_dir, "bare")
    send(log_dir, "no explicit prompt", task_id="bare")
    await registry.continue_task("bare", None, None, consume_followups=True)
    assert spawns.prompts[1].endswith(followups.CLOSING_LINE)


async def test_an_identical_continue_retry_returns_the_same_successor_without_reconstruction(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    put_record(log_dir)
    send(log_dir, "consumed by the first call")
    first = await registry.continue_task(SRC, "same", None, consume_followups=True)
    with pytest.raises(inbox.SendRefused):
        send(log_dir, "sealed")
    again = await registry.continue_task(SRC, "same", None, consume_followups=True)
    assert again is first and len(spawns.calls) == 1  # the queue is not read again
    # Another server (no live registry entry) is told which successor it is, not handed a rebuild.
    other = TaskRegistry(log_dir=log_dir, owner=OWNER)
    Spawns(other, pytest.MonkeyPatch())
    with pytest.raises(ValueError, match=f"Continuation already exists: {first.task_id}"):
        await other.continue_task(SRC, "same", None, consume_followups=True)
    with pytest.raises(ValueError, match="different caller request"):
        await registry.continue_task(SRC, "different", None, consume_followups=True)


async def test_a_continue_retry_after_a_release_reserves_the_current_queue(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    put_record(log_dir)
    gone = send(log_dir, "withdrawn in between")["message_id"]
    send(log_dir, "kept")
    spawns.pre_exec = OSError("no executable")
    with pytest.raises(OSError):
        await registry.continue_task(SRC, "same", None, consume_followups=True)
    reservation = json.loads((log_dir / f"{SRC}.continuation.json").read_text())
    assert reservation["state"] == "not_started" and statuses(log_dir) == ["queued", "queued"]
    followups.cancel(log_dir, SRC, gone)
    late = send(log_dir, "queued meanwhile")["message_id"]
    spawns.pre_exec = None
    child = await registry.continue_task(SRC, "same", None, consume_followups=True)
    assert child.task_id == reservation["task_id"]
    assert "withdrawn in between" not in spawns.prompts[-1]
    assert spawns.prompts[-1].index("kept") < spawns.prompts[-1].index("queued meanwhile")
    assert spawns.calls[-1]["message_ids"][-1] == late


async def test_continue_task_failure_cases_resolve_like_resume(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    put_record(log_dir)
    send(log_dir, "x")
    spawns.after_register = RuntimeError("after registration")
    with pytest.raises(RuntimeError):
        await registry.continue_task(SRC, None, None, consume_followups=True)
    assert statuses(log_dir) == ["consumed"]
    put_record(log_dir, "amb")
    send(log_dir, "y", task_id="amb")
    spawns.after_register = None
    spawns.ambiguous = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await registry.continue_task("amb", None, None, consume_followups=True)
    assert statuses(log_dir, "amb") == ["delivery_unknown"]


async def test_an_automatic_continuation_never_consumes_and_budget_exhaustion_leaves_the_queue(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    from polybridge.backends.base import Accumulator
    from polybridge import task_continuation

    put_record(log_dir, auto_continue=1)
    send(log_dir, "stays queued")
    source = make_task(log_dir, SRC, session_id=f"sess-{SRC}", finished=True)
    source.auto_continue = 1
    source.exit_code = 0
    source.acc = Accumulator(terminal={"subtype": "error_max_turns"})
    registry._tasks[SRC] = source
    child = await task_continuation.continue_task(registry, SRC, automatic=True, consume_followups=True)
    assert spawns.calls[0]["message_ids"] is None and "stays queued" not in spawns.prompts[0]
    assert statuses(log_dir) == ["queued"]
    # The leftover is reported once someone looks: the session has moved on.
    followups._SUCCESSOR_CACHE.clear()  # a status read just above may have cached "no successor"
    view = followups.view(log_dir, SRC)
    assert view["items"][0]["status"] == "undeliverable" and child.task_id in view["items"][0]["reason"]
    # An exhausted budget leaves a message queued on the last task of the chain.
    put_record(log_dir, "last", auto_continue=0)
    send(log_dir, "last words", task_id="last")
    with pytest.raises(ValueError, match="not eligible"):
        await task_continuation.continue_task(registry, "last", automatic=True)
    assert followups.view(log_dir, "last")["items"][0]["status"] == "queued"


async def test_a_stopped_chain_refuses_continue_but_resume_may_still_consume(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    record = put_record(log_dir)
    send(log_dir, "still wanted")
    (log_dir / f"{SRC}.continuation-stopped.json").write_text("{}")
    with pytest.raises(ValueError, match="cancelled"):
        await registry.continue_task(SRC, None, None, consume_followups=True)
    assert statuses(log_dir) == ["queued"]
    await registry.resume_record(record, None, consume_followups=True)
    assert statuses(log_dir) == ["consumed"]


# --- retention ------------------------------------------------------------------------------------------------------

def sweep(log_dir: Path) -> dict:
    return retention.sweep(log_dir, 30, datetime.now(timezone.utc))


def test_retention_keeps_tasks_with_outstanding_messages_and_never_deletes_the_lock(log_dir: Path) -> None:
    put_record(log_dir, age_days=40)
    put_record(log_dir, "quiet", age_days=40)
    mid = send(log_dir, "still waiting")["message_id"]
    stats = sweep(log_dir)
    assert store.read(log_dir, SRC) is not None and followups.state_path(log_dir, SRC).exists()
    assert store.read(log_dir, "quiet") is None and stats["deleted_tasks"] == 1
    followups.cancel(log_dir, SRC, mid)
    assert sweep(log_dir)["deleted_tasks"] == 1
    assert store.read(log_dir, SRC) is None
    assert not followups.state_path(log_dir, SRC).exists()
    assert followups.lock_path(log_dir, SRC).exists()  # a permanent lock file
    with pytest.raises(inbox.SendRefused) as caught:  # the task is gone: refused, not lost
        send(log_dir)
    assert caught.value.code == "unknown_task"


def test_retention_does_not_block_on_delivery_unknown_or_undeliverable(log_dir: Path) -> None:
    put_record(log_dir, age_days=40)
    send(log_dir, "unknown fate")
    doc = state_doc(log_dir)
    doc["items"][0].update(status="delivery_unknown", reason="x")
    followups.state_path(log_dir, SRC).write_text(json.dumps(doc))
    assert sweep(log_dir)["deleted_tasks"] == 1 and store.read(log_dir, SRC) is None


def test_a_send_landing_after_the_protection_scan_keeps_the_task(
    log_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    put_record(log_dir, age_days=40)
    real = followups.protected_task_ids

    def scan_then_send(directory: Path):
        found = real(directory)  # nothing queued yet ...
        send(directory, "acknowledged after the scan")  # ... then a send lands
        return found

    monkeypatch.setattr(followups, "protected_task_ids", scan_then_send)
    stats = sweep(log_dir)
    assert stats["kept_followups"] == 1 and stats["deleted_tasks"] == 0
    assert followups.view(log_dir, SRC)["items"][0]["text"] == "acknowledged after the scan"
    assert store.read(log_dir, SRC) is not None


def test_a_send_racing_the_delete_is_never_acknowledged_and_lost(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    put_record(log_dir, age_days=40)
    attempts: list[object] = []

    def sender_while_the_reaper_holds_the_lock() -> None:
        # The reaper holds the follow-up lock here: a sender cannot be acknowledged until it lets go.
        try:
            attempts.append(send(log_dir, "racing", timeout=0.2))
        except inbox.SendRefused as exc:
            attempts.append(exc.code)

    monkeypatch.setattr(followups, "_after_delete_lock", sender_while_the_reaper_holds_the_lock)
    stats = sweep(log_dir)
    assert attempts == ["lock_timeout"] and stats["deleted_tasks"] == 1
    monkeypatch.setattr(followups, "_after_delete_lock", None)
    assert store.read(log_dir, SRC) is None
    with pytest.raises(inbox.SendRefused) as caught:  # after the record is gone: refused, never lost
        send(log_dir, "too late")
    assert caught.value.code == "unknown_task"
    assert not followups.state_path(log_dir, SRC).exists()


def test_a_successor_is_protected_while_a_reservation_names_it_and_cannot_be_reconciled(log_dir: Path) -> None:
    reserved_ledger(log_dir, "succ", {"pid": 1, "start_time": "x", "markers": []})
    assert followups.protected_task_ids(log_dir) == {SRC, "succ"}
    put_record(log_dir, SRC, age_days=40)
    put_record(log_dir, "succ", age_days=40, parent_task_id=SRC, session_id="other-session")
    holder = os.open(followups.lock_path(log_dir, SRC), os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)  # busy: it cannot be reconciled this sweep, so it stays protected
    try:
        sweep(log_dir)
    finally:
        os.close(holder)
    assert store.read(log_dir, "succ") is not None and store.read(log_dir, SRC) is not None
    assert statuses(log_dir) == ["reserved"]


def test_a_crash_leftover_reservation_stops_pinning_its_task_without_any_further_activity(
    log_dir: Path, identities
) -> None:
    identities.dead(777_777)
    reserved_ledger(log_dir, "never-started", {"pid": 777_777, "start_time": "x", "markers": []})
    put_record(log_dir, SRC, age_days=40)  # the record the ledger belongs to, old and settled
    assert followups.protected_task_ids(log_dir) == {SRC, "never-started"}
    assert sweep(log_dir)["deleted_tasks"] == 1  # reconciled to delivery_unknown, then reaped
    assert store.read(log_dir, SRC) is None


def test_messages_superseded_by_an_automatic_continuation_stop_pinning_their_task(log_dir: Path) -> None:
    put_record(log_dir, SRC, age_days=40)
    send(log_dir, "left behind")
    put_record(log_dir, "auto-child", age_days=40, continuation_of_task_id=SRC, session_id="fresh")
    assert SRC in followups.protected_task_ids(log_dir)
    stats = sweep(log_dir)
    assert stats["deleted_tasks"] == 2
    assert store.read(log_dir, SRC) is None and store.read(log_dir, "auto-child") is None


def test_retention_protects_a_task_whose_ledger_cannot_be_read(log_dir: Path) -> None:
    put_record(log_dir, age_days=40)
    followups.state_path(log_dir, SRC).write_text("{broken")
    assert followups.protected_task_ids(log_dir) == {SRC}
    assert sweep(log_dir)["deleted_tasks"] == 0


# --- review round 1: transport, loop and cross-lock behaviour --------------------------------------------------

@pytest.mark.parametrize("backend_name", ["codex", "opencode", "vibe"])
async def test_a_continue_task_composition_over_32_kib_is_carried_whole(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns, backend_name: str
) -> None:
    for explicit in (None, "Finish up."):
        task_id = f"big-{backend_name}-{explicit is None}"
        put_record(log_dir, task_id, backend=backend_name, max_turns=None)
        for n in range(3):
            send(log_dir, f"{n}" + "w" * 20_000, task_id=task_id)
        await registry.continue_task(task_id, explicit, None, consume_followups=True)
        prompt, invocation = spawns.prompts[-1], spawns.invocations[-1]
        assert len(prompt.encode()) > 32 * 1024
        assert invocation.stdin_mode == "pipe_once" and invocation.initial_input == prompt.encode()
        if explicit is None:
            assert prompt.endswith(followups.CLOSING_LINE) and prompt == prompt.strip()
        else:
            assert prompt.endswith("Caller follow-up:\nFinish up.") and prompt == prompt.strip()


async def test_ledger_lock_contention_never_blocks_the_loop_even_while_cancelled(
    log_dir: Path
) -> None:
    put_record(log_dir)
    send(log_dir, "queued")
    holder = os.open(followups.lock_path(log_dir, SRC), os.O_CREAT | os.O_RDWR)
    fcntl.flock(holder, fcntl.LOCK_EX)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    ticking = asyncio.ensure_future(ticker())
    attempt = asyncio.ensure_future(followups.begin(log_dir, SRC, "succ", OWNER))
    try:
        await asyncio.sleep(0.2)
        before_cancel = ticks
        attempt.cancel()
        await asyncio.sleep(0.2)
        assert before_cancel >= 5 and ticks - before_cancel >= 5  # responsive while waiting and cancelling
        fcntl.flock(holder, fcntl.LOCK_UN)
        with pytest.raises(asyncio.CancelledError):
            await attempt
    finally:
        os.close(holder)
        ticking.cancel()
    # A reservation the cancelled worker may have made was released, off the loop.
    assert statuses(log_dir) == ["queued"] and state_doc(log_dir)["sealed_by"] is None


async def test_a_resume_and_a_continue_racing_on_one_source_consume_each_message_once(
    log_dir: Path, registry: TaskRegistry, spawns: Spawns
) -> None:
    """Different locks (session vs continuation chain): the ledger lock and the seal order them."""
    record = put_record(log_dir)
    send(log_dir, "shared message")
    spawns.finished = True
    first = [True]

    async def during() -> None:
        if first[0]:
            first[0] = False
            await asyncio.sleep(0.2)  # hold one reservation open while the other path runs

    spawns.during = during
    await asyncio.gather(
        registry.resume_record(record, "resume prompt", consume_followups=True),
        registry.continue_task(SRC, "continue prompt", None, consume_followups=True),
    )
    assert len(spawns.prompts) == 2
    assert sum("shared message" in p for p in spawns.prompts) == 1
    assert sum(bool(call["message_ids"]) for call in spawns.calls) == 1
    assert statuses(log_dir) == ["consumed"]


async def test_a_recovered_task_with_a_ledger_is_read_off_the_event_loop(
    log_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    put_record(log_dir)
    send(log_dir, "waiting")
    real = followups.view
    import time as _time

    def slow_view(*args, **kwargs):
        _time.sleep(0.3)  # a slow disk or a big scan; must not stall the loop
        return real(*args, **kwargs)

    monkeypatch.setattr(followups, "view", slow_view)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    ticking = asyncio.ensure_future(ticker())
    try:
        payload = await server.get_task_status(SRC)
    finally:
        ticking.cancel()
    assert payload["queued_followups"]["items"][0]["text"] == "waiting"
    assert ticks >= 10
