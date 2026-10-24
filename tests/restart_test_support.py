"""Deterministic process helpers for restart and supervision regression tests.

The helpers deliberately exercise the production Claude backend's normal executable lookup.  A
temporary ``claude`` entry point delegates to the checked-in stream-json fake, so tests do not
register an arbitrary Python backend or pickle an executable object into a supervisor request.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import shlex
import sys
from pathlib import Path
from typing import AsyncIterator


FAKE_AGENT = Path(__file__).with_name("fake_claude_agent.py")


def install_fake_claude(tmp_path: Path, monkeypatch, *, session: str = "restart-session") -> Path:
    """Put a real executable named ``claude`` in a temporary PATH prefix.

    The wrapper ignores Claude CLI flags and invokes the checked-in fake with a stable session
    identifier.  This is intentional: argument construction remains owned by the real Claude
    backend, while process I/O and exit behavior stay deterministic and token-free.
    """

    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir(exist_ok=True)
    executable = bin_dir / "claude"
    executable.write_text(
        "#!/bin/sh\n"
        "if [ -n \"${HARFLOW_FAKE_CLAUDE_SPAWN_LOG:-}\" ]; then echo $$ >> \"$HARFLOW_FAKE_CLAUDE_SPAWN_LOG\"; fi\n"
        "session=${HARFLOW_FAKE_CLAUDE_SESSION:-restart-session}\n"
        "prev=\"\"\n"
        "for arg in \"$@\"; do\n"
        "  if [ \"$prev\" = \"--session-id\" ]; then session=\"$arg\"; fi\n"
        "  prev=\"$arg\"\n"
        "done\n"
        f"exec {shlex.quote(sys.executable)} {shlex.quote(str(FAKE_AGENT))} \"$session\"\n"
    )
    executable.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    monkeypatch.setenv("HARFLOW_FAKE_CLAUDE_SESSION", session)
    monkeypatch.setenv("HARFLOW_FAKE_CLAUDE_SPAWN_LOG", str(tmp_path / "claude-spawns.log"))
    return executable


def encode_user_message(text: str) -> bytes:
    """Encode one Claude stream-json user message without changing caller whitespace."""

    return (json.dumps({"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": text}]}}) + "\n").encode()


async def read_events(stream: asyncio.StreamReader) -> AsyncIterator[dict]:
    """Yield JSON events until EOF, preserving malformed lines as a test failure."""

    async for raw in stream:
        line = raw.decode()
        if not line.strip():
            continue
        yield json.loads(line)


async def terminate_process(process: asyncio.subprocess.Process, *, sig: int = signal.SIGKILL) -> None:
    """Best-effort bounded cleanup for a process used by a fault probe."""

    if process.returncode is not None:
        return
    try:
        process.send_signal(sig)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=3)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
