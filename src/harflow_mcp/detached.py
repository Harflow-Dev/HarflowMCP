"""The fork-and-handshake lifecycle behind `harflow-ctl run` and `harflow-ctl resume` (A4.2).

A task needs a process to own it for as long as it runs — to drain its pipes (an unread pipe blocks
the agent) and publish its outcome — but the Monitor app wants a CLI call that returns as soon as
the task exists. So the command forks:

* the **child** acquires its selected-root lease, then points stdin at /dev/null and stdout/stderr at the selected state root's `ctl.log`, so
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

from .state_access import require_state_path
from .state_ownership import state_session, current_owned, assert_owned, StateOwnershipError

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


class _Handshake:
    """One idempotently closed pipe endpoint shared by child setup and runtime."""

    def __init__(self, descriptor: int):
        self.descriptor: int | None = descriptor

    def close(self) -> None:
        descriptor, self.descriptor = self.descriptor, None
        if descriptor is not None:
            os.close(descriptor)

    def send(self, message: dict[str, Any]) -> None:
        if self.descriptor is None:
            return
        try:
            os.write(self.descriptor, (json.dumps(message) + "\n").encode("utf-8"))
        except OSError:
            log.warning("could not report to the waiting harflow-ctl")
        finally:
            self.close()


def error_doc(exc: BaseException) -> dict[str, str]:
    """A stable `{code, message}` for anything starting or resuming a task can raise."""
    if isinstance(exc, StateOwnershipError):
        return {"code": getattr(exc, "code", "state_incompatible"), "message": str(exc)}
    from . import backends
    from .resources import ResourceError
    from .tasks import RepoUnavailableError, SessionBusyError, SessionUnknownError
    if isinstance(exc, ResourceError):
        return {"code": exc.code, "message": str(exc)}

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
    if isinstance(exc, ValueError) and getattr(exc, "harflow_not_started", False):
        # Refused inside a boundary that cannot have created a process: the caller's request was
        # invalid, and retrying it unchanged cannot strand a running agent.
        return {"code": "invalid_params", "message": str(exc)}
    return {"code": "start_failed", "message": f"{type(exc).__name__}: {exc}"}


def run_detached(
    action: Action,
    *,
    log_path: Path,
    registry_factory: Callable[[], Any],
    log_dir: Path | None = None,
    timeout: float = HANDSHAKE_TIMEOUT_SECONDS,
) -> Outcome:
    """Fork; the child runs `action(registry)` (which returns the started `Task`) and owns the task
    until it settles; the parent returns once the handshake arrives, or after `timeout`.

    `log_dir` supplies the already-selected task directory without constructing a parent registry.
    The child factory must select the same layout; embedding callers may omit it for preflight
    discovery through their factory.
    """
    from .state_layout import require_dispatch_layout, child_environment
    selected = require_dispatch_layout(log_dir if log_dir is not None else registry_factory().log_dir)
    def selected_factory():
        registry = registry_factory()
        if require_dispatch_layout(registry.log_dir) != selected:
            from .state_layout import UnsupportedStateLayout
            raise UnsupportedStateLayout("Unsupported state layout: detached child root differs from its selected parent layout")
        return registry
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:  # child — never returns
        code = 1
        channel = _Handshake(write_fd)
        try:
            os.close(read_fd)
            os.environ.update(child_environment(selected))
            with state_session(selected):
                _detach_stdio(log_path, layout=selected)
                os.setsid()
                code = asyncio.run(_child(action, channel, selected_factory))
        except StateOwnershipError as exc:
            channel.send({"error": error_doc(exc)})
            print(f"harflow-ctl: error: {exc}", file=sys.stderr)
        except BaseException:
            try:
                traceback.print_exc()
            except Exception:
                pass
        finally:
            channel.close()
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
    if reaped:
        fate = "was stopped"
    else:
        fate = (
            f"was sent SIGTERM but had not exited after {REAP_GRACE_SECONDS:.0f} s (pid {pid}); it "
            "was not killed, since it may still be stopping a task it started"
        )
    message = (
        f"the task's owning process did not report within {timeout:.0f} s and {fate}; a task may "
        "or may not have been started — check `harflow-ctl list`"
    )
    return Outcome("unknown", {"message": message}, pid)


def _detach_stdio(log_path: Path, *, layout) -> None:
    require_state_path(layout, log_path)
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


async def _wait_started(registry: Any) -> None:
    while True:
        continuations = list(getattr(registry, "_continuations", ()))
        pending = [task for task in registry.list() if not task.finished]
        if not continuations and not pending:
            return
        await asyncio.gather(*continuations, *(task.done.wait() for task in pending))


async def _release_started(registry: Any) -> None:
    """Cancel every task this process started, and do not return while any is unsettled.

    A cancel can fail (a phase file that cannot be written refuses before signalling), and exiting
    then would leave the agent running with nobody reading its output. So this keeps retrying, and
    meanwhile keeps the loop — and with it the task's drainers and monitor — alive. A repeated
    signal does not interrupt it.
    """
    me = asyncio.current_task()
    # Stop queued continuations before cancelling the subprocesses they may have created.
    continuations = list(getattr(registry, "_continuations", ()))
    for continuation in continuations:
        continuation.cancel()
    if continuations:
        await asyncio.gather(*continuations, return_exceptions=True)
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


async def _child(action: Action, write_fd: int | _Handshake, registry_factory: Callable[[], Any]) -> int:
    current_owned()
    channel = write_fd if isinstance(write_fd, _Handshake) else _Handshake(write_fd)
    # The fork copied the parent's cached identity; the owner of these tasks is this process.
    identity.own_identity.cache_clear()
    logging.basicConfig(
        level=os.environ.get("HARFLOW_LOG_LEVEL", "INFO").upper(),
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
    from .state_layout import require_dispatch_layout
    require_dispatch_layout(registry.log_dir)
    assert_owned(require_dispatch_layout(registry.log_dir))
    send = channel.send

    try:
        try:
            task = await action(registry)
        except asyncio.CancelledError:
            raise
        except StateOwnershipError as exc:
            log.error("could not start the task: %s", exc)
            send({"error": error_doc(exc)})
            return 1
        except Exception as exc:
            log.warning("could not start the task", exc_info=True)
            send({"error": error_doc(exc)})
            return 1
        send({"task_id": task.task_id})
        from .task_resources import PendingLaunch
        if isinstance(task, PendingLaunch):
            # The launch owner, not this process, holds the wait and will own the agent: nothing here
            # has a task to supervise, and staying would only outlive the acknowledgement.
            log.info("launch %s is %s under its own owner; leaving it there", task.task_id, task.launch_state)
            return 0
        log.info("owning task %s until it settles", task.task_id)
        await task.done.wait()
        # Continuation callbacks are scheduled by the terminal monitor before it yields.
        # Own their reservations and all successors until their drainers settle as well.
        await _wait_started(registry)
        log.info("task %s settled as %s", task.task_id, task.status)
        return 0
    except asyncio.CancelledError:
        main.uncancel()
        log.info("stopped by a signal; cancelling what this process started")
        await _release_started(registry)
        return CANCELLED_EXIT
    finally:
        channel.close()
