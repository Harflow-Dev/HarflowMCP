"""Pins the claude CLI behaviour live input is built on. Real runs; these spend real tokens.

    PB_INTEGRATION=1 uv run pytest tests/test_live_input_real.py

Stage A3 of the monitor plan keeps a claude run's stdin open (`--input-format stream-json`) so a
caller can add messages mid-run, and closes it once the agent is idle so the run still settles. Every
rule that design relies on is a claim about the CLI, not about polybridge, so each gets a raw-CLI test
here that fails loudly if a claude update changes it:

* a positional prompt next to `--input-format stream-json` is ignored, so the prompt goes on stdin;
* a message sent mid-turn is folded into that turn (one `result`), and EOF after a `result` exits 0;
* background-task event names: `system/task_started` with `is_backgrounded: true` (a *foreground*
  Bash emits `task_started` too, with `is_backgrounded: false`), `system/task_updated` whose
  `patch.status` goes terminal, and `system/task_notification`;
* with stdin open, a finished background task makes claude start a follow-up turn on its own;
* EOF while a background task runs kills it (`patch.status: "killed"`, notification `stopped`);
* `--resume` accepts the same live-input shape.

Measured 2026-09-25 on claude 2.1.281 (haiku) — the event names and statuses above are exactly what
that run produced. `CLAUDE*` variables are stripped from the child's environment so the probe is not
treated as a child of whatever session runs the suite.

The background tests run `--permission-mode bypassPermissions` inside a throwaway `tmp_path` repo:
headless `acceptEdits` cannot approve a chained Bash command, and the command is a harmless `sleep`.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("PB_INTEGRATION"),
        reason="set PB_INTEGRATION=1 to spawn real agent runs (spends real tokens)",
    ),
]

MODEL = os.environ.get("PB_LIVE_TEST_MODEL", "haiku")
RUN_TIMEOUT_SECONDS = 240.0
# Measured ~1.2 s from EOF to exit; generous so a slow machine does not flake.
EXIT_AFTER_EOF_SECONDS = 20.0

BACKGROUND_TERMINAL = {"completed", "failed", "killed", "stopped", "cancelled", "error"}


def _message(text: str) -> bytes:
    line = {"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": text}]}}
    return (json.dumps(line) + "\n").encode()


def _env() -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if not key.startswith("CLAUDE")}


def _argv(mode: str, *session: str) -> list[str]:
    return [
        "claude",
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--input-format",
        "stream-json",
        "--permission-mode",
        mode,
        "--model",
        MODEL,
        *session,
    ]


class _Run:
    """One raw claude process with a stdin pipe, its events collected as they arrive."""

    def __init__(self, proc: asyncio.subprocess.Process) -> None:
        self.proc = proc
        self.events: list[dict[str, Any]] = []
        self.changed = asyncio.Event()
        self.reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        assert self.proc.stdout is not None
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                break
            try:
                self.events.append(json.loads(line))
            except ValueError:
                continue
            self.changed.set()

    def send(self, text: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(_message(text))

    async def wait_for(self, predicate, timeout: float = RUN_TIMEOUT_SECONDS) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        seen = 0
        while True:
            for event in self.events[seen:]:
                if predicate(event):
                    return event
            seen = len(self.events)
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self.reader.done():
                pytest.fail(f"event never arrived; saw {[_kind(e) for e in self.events]}")
            self.changed.clear()
            try:
                await asyncio.wait_for(self.changed.wait(), remaining)
            except (asyncio.TimeoutError, TimeoutError):
                pass

    def results(self) -> list[dict[str, Any]]:
        return [event for event in self.events if event.get("type") == "result"]

    async def close_and_wait(self) -> int:
        assert self.proc.stdin is not None
        self.proc.stdin.close()
        code = await asyncio.wait_for(self.proc.wait(), EXIT_AFTER_EOF_SECONDS)
        await self.reader
        return code

    def kill(self) -> None:
        if self.proc.returncode is None:
            try:
                os.killpg(self.proc.pid, 9)
            except ProcessLookupError:
                pass


def _kind(event: dict[str, Any]) -> str:
    return f"{event.get('type')}/{event.get('subtype')}"


async def _spawn(cwd: Path, mode: str = "plan", *session: str) -> _Run:
    if not session:
        session = ("--session-id", str(uuid.uuid4()))
    proc = await asyncio.create_subprocess_exec(
        *_argv(mode, *session),
        cwd=str(cwd),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env=_env(),
        limit=8 * 1024 * 1024,
        start_new_session=True,
    )
    return _Run(proc)


def _is_result(event: dict[str, Any]) -> bool:
    return event.get("type") == "result"


def _system(subtype: str):
    return lambda event: event.get("type") == "system" and event.get("subtype") == subtype


@pytest.fixture(autouse=True)
def _claude_installed() -> None:
    from shutil import which

    if which("claude") is None:
        pytest.skip("the claude CLI is not on PATH")


async def test_a_positional_prompt_is_ignored_under_stream_json_input(git_repo: Path) -> None:
    """Why the prompt must ride on stdin: with `--input-format stream-json` a positional prompt is
    not run at all. Stdin is closed at once, so a CLI that did run it would emit a `result`."""
    result = await asyncio.to_thread(
        subprocess.run,
        [*_argv("plan", "--session-id", str(uuid.uuid4())), "--", "Reply with exactly: ack"],
        cwd=git_repo,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        env=_env(),
        timeout=RUN_TIMEOUT_SECONDS,
    )
    events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert result.returncode == 0, result.stderr
    assert not [event for event in events if _is_result(event)], events


async def test_a_mid_turn_message_is_folded_into_the_running_turn(git_repo: Path) -> None:
    run = await _spawn(git_repo, "bypassPermissions")
    try:
        run.send(
            "Run the bash command `sleep 6` in the foreground (not in the background), then "
            "answer: what is 2+2? Reply briefly."
        )
        # Sent while the foreground sleep runs, i.e. mid-turn.
        started = await run.wait_for(_system("task_started"))
        assert started.get("is_backgrounded") is False
        run.send("Also: what is 3+3?")
        result = await run.wait_for(_is_result)
        await asyncio.sleep(3)
        code = await run.close_and_wait()
    finally:
        run.kill()

    assert code == 0
    assert len(run.results()) == 1, [r.get("result") for r in run.results()]
    assert "4" in result["result"] and "6" in result["result"], result["result"]
    # The foreground task's own closing notification, same event name as a background one.
    notification = next(e for e in run.events if _system("task_notification")(e))
    assert notification["task_id"] == started["task_id"]
    assert notification["status"] == "completed"
    # The injected text is not echoed back as a user event.
    user_texts = [
        block
        for event in run.events
        if event.get("type") == "user"
        for block in (event.get("message") or {}).get("content") or []
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    assert user_texts == []


async def test_background_task_events_and_the_follow_up_turn(git_repo: Path) -> None:
    run = await _spawn(git_repo, "bypassPermissions")
    try:
        run.send(
            "Use the Bash tool with run_in_background=true to run exactly: sleep 6; echo BGDONE . "
            "Then immediately reply with the single word STARTED and end your turn. Do not wait "
            "for it or check on it."
        )
        started = await run.wait_for(_system("task_started"))
        first = await run.wait_for(_is_result)
        updated = await run.wait_for(_system("task_updated"))
        notification = await run.wait_for(_system("task_notification"))
        # stdin still open: claude starts a follow-up turn for the finished task by itself.
        await run.wait_for(lambda e: _is_result(e) and e is not first)
        code = await run.close_and_wait()
    finally:
        run.kill()

    assert started["is_backgrounded"] is True
    assert isinstance(started["task_id"], str) and started["task_id"]
    assert updated["task_id"] == started["task_id"]
    assert updated["patch"]["status"] == "completed"
    assert notification["task_id"] == started["task_id"]
    assert notification["status"] == "completed"
    assert notification["status"] in BACKGROUND_TERMINAL
    assert len(run.results()) == 2
    assert code == 0


async def test_eof_while_a_background_task_runs_kills_it(git_repo: Path) -> None:
    run = await _spawn(git_repo, "bypassPermissions")
    try:
        run.send(
            "Use the Bash tool with run_in_background=true to run exactly: sleep 60; echo BGDONE . "
            "Then immediately reply with the single word STARTED and end your turn. Do not wait "
            "for it or check on it."
        )
        started = await run.wait_for(_system("task_started"))
        await run.wait_for(_is_result)
        code = await run.close_and_wait()
    finally:
        run.kill()

    assert started["is_backgrounded"] is True
    updated = next(e for e in run.events if _system("task_updated")(e))
    notification = next(e for e in run.events if _system("task_notification")(e))
    assert updated["task_id"] == started["task_id"]
    assert updated["patch"]["status"] == "killed"
    assert notification["status"] == "stopped"
    # Nobody ever sees the killed task's result: no follow-up turn.
    assert len(run.results()) == 1
    assert code == 0


async def test_resume_accepts_live_input(git_repo: Path) -> None:
    session_id = str(uuid.uuid4())
    first = await _spawn(git_repo, "plan", "--session-id", session_id)
    try:
        first.send("Remember the word PELICAN. Reply with exactly: ok")
        await first.wait_for(_is_result)
        assert await first.close_and_wait() == 0
    finally:
        first.kill()

    second = await _spawn(git_repo, "plan", "--resume", session_id)
    try:
        second.send("Which word did I ask you to remember? Reply with just the word.")
        result = await second.wait_for(_is_result)
        assert await second.close_and_wait() == 0
    finally:
        second.kill()

    assert result.get("session_id") == session_id
    assert "PELICAN" in result["result"].upper()
