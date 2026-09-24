"""The fork-and-handshake lifecycle behind `polybridge-ctl run` and `polybridge-ctl resume` (A4.2).

A task needs a process to own it for as long as it runs — to drain its pipes (an unread pipe blocks
the agent) and publish its outcome — but the Monitor app wants a CLI call that returns as soon as
the task exists. So the command forks:

* the **child** points stdin at /dev/null and stdout/stderr at `~/.polybridge/ctl.log` *first*, so
  whatever reads the parent's stdout (the app) sees EOF the moment the parent exits; then it calls
  `os.setsid()`, builds a `TaskRegistry` inside its own running loop — no retention, no open-app —
  starts or resumes the task, sends one line down a pipe (`{"task_id"}` or `{"error"}`), and stays
  to own the task until it settles. SIGTERM/SIGINT cancel whatever it started.
* the **parent** waits on that pipe for `HANDSHAKE_TIMEOUT_SECONDS`, prints what arrived, and exits
  with a matching status. Nothing arriving (a hang, or a child that died first) is reported as
  `unknown` — not `error`, because a task may already exist — after the child is sent SIGTERM and,
  within `REAP_GRACE_SECONDS`, reaped. The child cancels anything it had already spawned on its way
  out, and **keeps owning any task that has not settled** — exiting would leave the agent running
  with nobody draining its pipes. For the same reason the parent never SIGKILLs it: a child still
  stopping its task when the grace runs out is left to finish, and the answer says so.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import select
import signal
import sys
import time
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import identity

log = logging.getLogger(__name__)

HANDSHAKE_TIMEOUT_SECONDS = 30.0
REAP_GRACE_SECONDS = 15.0
RELEASE_RETRY_SECONDS = 5.0
CANCELLED_EXIT = 143

Action = Callable[[Any], Awaitable[Any]]


@dataclass(frozen=True)
class Outcome:
    """What the parent learned: `kind` is `"task"` (payload `{"task_id"}`), `"error"` (payload
    `{"code", "message"}`) or `"unknown"` (payload `{"message"}`)."""

    kind: str
    payload: dict[str, Any]
    child_pid: int


def error_doc(exc: BaseException) -> dict[str, str]:
    """A stable `{code, message}` for anything starting or resuming a task can raise."""
    from . import backends
    from .tasks import RepoUnavailableError, SessionBusyError, SessionUnknownError

    try:
        from mcp import MCPError
    except Exception:  # pragma: no cover - mcp is a hard dependency
        MCPError = ()  # type: ignore[assignment]

    ordered: list[tuple[Any, str]] = [
        (MCPError, "invalid_params"),
        (SessionBusyError, "session_busy"),
        (SessionUnknownError, "session_unknown"),
        (RepoUnavailableError, "repo_unavailable"),
        (backends.NestedDispatchRefused, "nested_dispatch_refused"),
        (backends.UnsupportedCapability, "unsupported"),
        (backends.UnknownBackend, "unknown_backend"),
        (OSError, "spawn_failed"),
    ]
    for kind, code in ordered:
        if kind and isinstance(exc, kind):
            return {"code": code, "message": str(exc)}
    return {"code": "start_failed", "message": f"{type(exc).__name__}: {exc}"}


def run_detached(
    action: Action,
    *,
    log_path: Path,
    registry_factory: Callable[[], Any],
    timeout: float = HANDSHAKE_TIMEOUT_SECONDS,
) -> Outcome:
    """Fork; the child runs `action(registry)` (which returns the started `Task`) and owns the task
    until it settles; the parent returns once the handshake arrives, or after `timeout`."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # child — never returns
        code = 1
        try:
            os.close(read_fd)
            _detach_stdio(log_path)
            os.setsid()
            code = asyncio.run(_child(action, write_fd, registry_factory))
        except BaseException:
            try:
                traceback.print_exc()
            except Exception:
                pass
        finally:
            os._exit(code)

    os.close(write_fd)
    try:
        line = _read_line(read_fd, timeout)
    finally:
        os.close(read_fd)

    message: Any = None
    if line:
        try:
            message = json.loads(line)
        except ValueError:
            message = None
    if isinstance(message, dict) and isinstance(message.get("task_id"), str):
        return Outcome("task", {"task_id": message["task_id"]}, pid)
    if isinstance(message, dict) and isinstance(message.get("error"), dict):
        _reap(pid)
        return Outcome("error", message["error"], pid)

    reaped = _terminate(pid)
    message = (
        f"the task's owning process did not report within {timeout:.0f} s and was stopped; a task "
        "may or may not have been started — check `polybridge-ctl list`"
    )
    if not reaped:
        message += (
            f" (the owning process, pid {pid}, is still stopping what it started and keeps owning "
            "it until it settles)"
        )
    return Outcome("unknown", {"message": message}, pid)


def _detach_stdio(log_path: Path) -> None:
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    out = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(out, 1)
    os.dup2(out, 2)
    os.close(out)
    sys.stdout = os.fdopen(1, "w", buffering=1, closefd=False)
    sys.stderr = os.fdopen(2, "w", buffering=1, closefd=False)


def _read_line(fd: int, timeout: float) -> bytes:
    """Bytes up to the first newline, or whatever arrived before EOF or the deadline."""
    buffer = b""
    deadline = time.monotonic() + timeout
    while b"\n" not in buffer:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return b""
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            return b""
        chunk = os.read(fd, 4096)
        if not chunk:
            return buffer if b"\n" in buffer else b""
        buffer += chunk
    return buffer.split(b"\n", 1)[0]


def _reap(pid: int, grace: float | None = None) -> bool:
    """Wait for `pid` to exit, up to `grace` (default `REAP_GRACE_SECONDS`). True once reaped."""
    deadline = time.monotonic() + (REAP_GRACE_SECONDS if grace is None else grace)
    while True:
        try:
            done, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return True
        if done:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.05)


def _terminate(pid: int) -> bool:
    """SIGTERM the child — it cancels what it started — and reap it. Never SIGKILL: a child still
    busy stopping a task owns that task's pipes, and killing it would orphan the agent. Returns
    whether it was reaped within the grace."""
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    return _reap(pid)


async def _release_started(registry: Any) -> None:
    """Cancel every task this process started, and do not return while any is unsettled.

    A cancel can fail (a phase file that cannot be written refuses before signalling), and exiting
    then would leave the agent running with nobody reading its output. So this keeps retrying, and
    meanwhile keeps the loop — and with it the task's drainers and monitor — alive. A repeated
    signal does not interrupt it.
    """
    me = asyncio.current_task()
    while True:
        pending = [task for task in registry.list() if not task.finished]
        if not pending:
            return
        for task in pending:
            try:
                await registry.cancel_cascade(task.task_id)
            except asyncio.CancelledError:
                if me is not None:
                    me.uncancel()
            except Exception:
                log.warning("could not cancel task %s; still owning it", task.task_id, exc_info=True)
        pending = [task for task in registry.list() if not task.finished]
        if not pending:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*(task.done.wait() for task in pending)),
                timeout=RELEASE_RETRY_SECONDS,
            )
        except (TimeoutError, asyncio.TimeoutError):
            pass
        except asyncio.CancelledError:
            if me is not None:
                me.uncancel()


async def _child(action: Action, write_fd: int, registry_factory: Callable[[], Any]) -> int:
    # The fork copied the parent's cached identity; the owner of these tasks is this process.
    identity.own_identity.cache_clear()
    logging.basicConfig(
        level=os.environ.get("PB_LOG_LEVEL", "INFO").upper(),
        stream=sys.stderr,
        format="%(asctime)s %(levelname)-7s %(name)s[%(process)d]: %(message)s",
        force=True,
    )
    loop = asyncio.get_running_loop()
    main = asyncio.current_task()
    assert main is not None
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, main.cancel)

    registry = registry_factory()
    pending_fd: int | None = write_fd

    def send(message: dict[str, Any]) -> None:
        nonlocal pending_fd
        if pending_fd is None:
            return
        try:
            os.write(pending_fd, (json.dumps(message) + "\n").encode("utf-8"))
        except OSError:
            log.warning("could not report to the waiting polybridge-ctl", exc_info=True)
        finally:
            os.close(pending_fd)
            pending_fd = None

    try:
        try:
            task = await action(registry)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("could not start the task", exc_info=True)
            send({"error": error_doc(exc)})
            return 1
        send({"task_id": task.task_id})
        log.info("owning task %s until it settles", task.task_id)
        await task.done.wait()
        log.info("task %s settled as %s", task.task_id, task.status)
        return 0
    except asyncio.CancelledError:
        main.uncancel()
        log.info("stopped by a signal; cancelling what this process started")
        await _release_started(registry)
        return CANCELLED_EXIT
    finally:
        if pending_fd is not None:
            os.close(pending_fd)
