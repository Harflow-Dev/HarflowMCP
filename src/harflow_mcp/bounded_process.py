"""Run one short-lived command with a deadline and separate output caps.

The runner drains stdout and stderr in one selector loop, reads stdin from `/dev/null`, runs the command
in its own session and kills the whole process group when the deadline passes or either stream exceeds its
cap. It never raises for command problems: the outcome is a `Completed` with a `cause`.
"""
from __future__ import annotations

import os
import selectors
import signal
import subprocess
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

TIMEOUT = "timeout"
OUTPUT_CEILING = "output_ceiling"
SPAWN_FAILED = "spawn_failed"

# A command that closed both streams at the deadline edge has finished: give its exit a fixed moment.
EXIT_GRACE_SECONDS = 0.5
# After SIGKILL, how long to wait for the reap before handing it to a daemon thread.
REAP_SECONDS = 2.0
_CHUNK = 65536
_POLL_SECONDS = 0.05


@dataclass(frozen=True)
class Completed:
    """The outcome of one run. `returncode` is None whenever `cause` is set."""

    returncode: int | None
    stdout: bytes = b""
    stderr: bytes = b""
    cause: str | None = None  # timeout | output_ceiling | spawn_failed


def _kill(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        process.kill()
    except OSError:
        pass


def _reap(process: subprocess.Popen) -> None:
    try:
        process.wait()
    except Exception:
        pass


def run(
    argv: Sequence[str],
    *,
    env: Mapping[str, str],
    cwd: str,
    deadline: float,
    stdout_cap: int,
    stderr_cap: int,
) -> Completed:
    """Run `argv` until `deadline` (a `time.monotonic()` value), capturing at most the given bytes per stream.

    A stream that would exceed its cap stops the run with `output_ceiling`; the bytes within the cap are
    returned. Expiry stops it with `timeout`. A command that cannot be spawned yields `spawn_failed`.
    """
    if deadline - time.monotonic() <= 0:
        return Completed(None, cause=TIMEOUT)
    try:
        process = subprocess.Popen(
            list(argv), stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=dict(env), cwd=cwd, start_new_session=True,
        )
    except (OSError, ValueError):
        return Completed(None, cause=SPAWN_FAILED)
    out_fd, err_fd = process.stdout.fileno(), process.stderr.fileno()
    buffers: dict[int, bytearray] = {}
    caps: dict[int, int] = {}
    selector = selectors.DefaultSelector()
    cause: str | None = None
    try:
        for stream, cap in ((process.stdout, stdout_cap), (process.stderr, stderr_cap)):
            descriptor = stream.fileno()
            os.set_blocking(descriptor, False)
            selector.register(descriptor, selectors.EVENT_READ)
            buffers[descriptor] = bytearray()
            caps[descriptor] = cap
        open_streams = 2
        while open_streams and cause is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                cause = TIMEOUT
                break
            for key, _ in selector.select(min(remaining, _POLL_SECONDS)):
                descriptor = key.fd
                try:
                    chunk = os.read(descriptor, _CHUNK)
                except BlockingIOError:
                    continue
                except OSError:
                    chunk = b""
                if not chunk:
                    selector.unregister(descriptor)
                    open_streams -= 1
                    continue
                room = caps[descriptor] - len(buffers[descriptor])
                if len(chunk) > room:
                    buffers[descriptor] += chunk[:max(room, 0)]
                    cause = OUTPUT_CEILING
                    break
                buffers[descriptor] += chunk
        if cause is None:
            try:
                process.wait(timeout=max(deadline - time.monotonic(), EXIT_GRACE_SECONDS))
            except subprocess.TimeoutExpired:
                cause = TIMEOUT
    finally:
        selector.close()
        if cause is not None or process.poll() is None:
            _kill(process)
        try:
            process.wait(timeout=REAP_SECONDS)
        except subprocess.TimeoutExpired:
            # A process that survives SIGKILL is abandoned to a daemon reaper rather than waited on here.
            cause = cause or TIMEOUT
            threading.Thread(target=_reap, args=(process,), daemon=True).start()
        for stream in (process.stdout, process.stderr):
            try:
                stream.close()
            except OSError:
                pass
    return Completed(
        None if cause else process.returncode,
        bytes(buffers.get(out_fd, b"")), bytes(buffers.get(err_fd, b"")), cause,
    )
