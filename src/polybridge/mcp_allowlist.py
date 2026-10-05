"""Explicit, human-only persistence of harness-native global MCP approvals.

Native configuration semantics are supplied by Backend.mcp_approval.
"""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import re
import tempfile

from .backends import get

_ENTRY = re.compile(r"([A-Za-z0-9_-]+)/([A-Za-z0-9_.-]+|\*)\Z")


def config_path(backend: str) -> Path:
    return get(backend).mcp_approval.config_path()


def edit(backend: str, *, allow: str | None = None, remove: str | None = None) -> dict:
    if allow is not None and remove is not None:
        raise ValueError("Choose add or remove")
    entry = allow if allow is not None else remove
    if entry is not None and not _ENTRY.fullmatch(entry):
        raise ValueError("Use server/tool or server/* with valid MCP names")
    policy = get(backend).mcp_approval
    path = policy.config_path()
    if reason := policy.unsupported_reason():
        return dict(backend=backend, supported=False, config_path=str(path), entries=[], detail=reason)
    if entry is None:
        raw = path.read_text() if path.exists() else ""
        data = policy.parse(raw)
        return dict(backend=backend, supported=True, config_path=str(path), entries=policy.entries(data), detail=policy.detail_prefix + "Global rules apply to future sessions. Harness deny rules and project overrides still apply.")
    path.parent.mkdir(parents=True, exist_ok=True)
    backup = path.with_suffix(path.suffix + ".polybridge-backup")
    lock_path = path.with_suffix(path.suffix + ".polybridge.lock")
    if any(p.is_symlink() for p in (path, backup, lock_path)):
        raise ValueError("Refusing to edit symlinked harness configuration or backup")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        raw = path.read_text() if path.exists() else ""
        data = policy.parse(raw)
        updated = policy.update(raw, data, entry, allow=allow is not None)
        if path.exists() and path.read_text() != raw:
            raise ValueError("Harness configuration changed; reload and try again")
        if raw:
            _atomic_write(backup, raw, 0o600)
        _atomic_write(path, updated, path.stat().st_mode & 0o777 if path.exists() else 0o600)
        return dict(backend=backend, supported=True, config_path=str(path), entries=policy.entries(data), detail="Saved global MCP approval rule. Start a new session or continue the builder to load it; project and deny rules may override it.")


def _atomic_write(path: Path, text: str, mode: int) -> None:
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
