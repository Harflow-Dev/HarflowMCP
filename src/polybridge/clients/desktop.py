"""The Claude desktop app: the one client with no CLI, so we edit its config ourselves.

This edits a file it does not own, so it merges rather than replaces, backs up first, refuses to
touch a config it cannot parse, and writes atomically.

Atomic *for readers* — a reader never sees half a file. It is not locked: the app can write between
our last comparison and the replace, and that write would be lost. The check narrows the window; it
does not close it.
"""

from __future__ import annotations

import dataclasses
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .base import Availability, Registration, Result, Runner, SetupError

MANUAL_INSTRUCTIONS = """
Add this to your MCP client's config by hand, using absolute paths:

  "mcpServers": {
    "polybridge": {
      "command": "<absolute path to polybridge-server>",
      "env": { "PATH": "<dir holding claude>:/usr/local/bin:/usr/bin:/bin" }
    }
  }
"""


def desktop_config_path() -> Path:
    if sys.platform == "darwin":
        return (
            Path.home()
            / "Library"
            / "Application Support"
            / "Claude"
            / "claude_desktop_config.json"
        )
    if sys.platform.startswith("linux"):
        return Path.home() / ".config" / "Claude" / "claude_desktop_config.json"
    raise SetupError(
        f"no known Claude desktop config location for platform {sys.platform!r}."
        + MANUAL_INSTRUCTIONS
    )


def server_entry(registration: Registration) -> dict[str, Any]:
    return {"command": registration.command, "env": {"PATH": registration.path_env}}


def read_raw(path: Path) -> str | None:
    """Current contents, or None if absent. Used both to parse and to detect concurrent edits."""
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None


def load_config(path: Path) -> dict[str, Any]:
    """Read an existing config, or return a fresh one. Never silently discards a malformed file."""
    raw = read_raw(path)
    if raw is None or not raw.strip():
        return {}
    try:
        config = json.loads(raw)
    except ValueError as exc:
        raise SetupError(
            f"{path} is not valid JSON ({exc}). Refusing to overwrite it — fix or move it first."
        ) from None
    if not isinstance(config, dict):
        raise SetupError(f"{path} does not contain a JSON object. Refusing to overwrite it.")
    return config


def merge_entry(config: dict[str, Any], key: str, entry: dict[str, Any]) -> dict[str, Any]:
    """Set our server key, leaving every other key's *value* as it was.

    Formatting is not preserved: the file is reserialized, so comments-by-convention and key order
    outside our own entry survive only as far as `json.dumps` reproduces them.
    """
    servers = config.get("mcpServers")
    if servers is not None and not isinstance(servers, dict):
        raise SetupError("existing 'mcpServers' is not a JSON object. Refusing to overwrite it.")

    merged = dict(config)
    merged["mcpServers"] = {**(servers or {}), key: entry}
    return merged


def back_up(path: Path) -> Path | None:
    """Copy the config aside, never overwriting an existing backup.

    Timestamps are only second-precise, so two runs in the same second would otherwise collide and
    destroy the very thing the backup exists to protect.
    """
    if not path.exists():
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    for suffix in ("", *(f".{n}" for n in range(1, 100))):
        backup = path.with_name(f"{path.name}.bak-{stamp}{suffix}")
        try:
            with open(backup, "xb") as target:
                target.write(path.read_bytes())
        except FileExistsError:
            continue
        shutil.copystat(path, backup)
        return backup
    raise SetupError(f"could not find an unused backup name beside {path}")


def write_config(path: Path, config: dict[str, Any]) -> None:
    """Replace the config atomically, carrying its Unix mode bits across.

    A temp file in the same directory plus os.replace means a reader never sees a half-written
    config. The mode is copied because NamedTemporaryFile creates 0600, which would otherwise
    silently tighten the file. Only the mode bits: ownership and ACLs are not reproduced.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(config, indent=2) + "\n"
    original_mode = path.stat().st_mode & 0o777 if path.exists() else None

    handle = tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
    )
    try:
        with handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        if original_mode is not None:
            os.chmod(handle.name, original_mode)
        os.replace(handle.name, path)
    except BaseException:
        Path(handle.name).unlink(missing_ok=True)
        raise


@dataclass(frozen=True)
class DesktopClient:
    """Registers by editing `claude_desktop_config.json` — the app has no CLI to delegate to."""

    key: str = "claude-desktop"
    label: str = "Claude desktop app"
    post_apply_note: str = "restart it to pick up the change."
    config_path: Path | None = None

    def with_config_path(self, config_path: Path) -> DesktopClient:
        """The capability `--desktop-config` needs. Its presence is what marks a client as editable."""
        return dataclasses.replace(self, config_path=config_path)

    def resolve_path(self) -> Path:
        path = self.config_path or desktop_config_path()
        # A symlinked config should have its target rewritten, not be replaced by a regular file.
        return Path(os.path.realpath(path)) if path.is_symlink() else path

    def availability(self) -> Availability:
        """Available wherever the config location is known — *not* "the app is installed".

        There is no reliable way to detect the desktop app: it can live outside /Applications, and its
        config directory does not exist until it has run once. Requiring the directory would skip a
        freshly installed app, which is the common case an installer exists for. So the entry is
        written on any supported platform, and the README says as much rather than implying detection.
        """
        try:
            return Availability(True, where=str(self.resolve_path()))
        except SetupError as exc:
            return Availability(False, reason=str(exc).splitlines()[0])

    def preview(self, registration: Registration) -> Result:
        path = self.resolve_path()
        merged = merge_entry(load_config(path), registration.key, server_entry(registration))
        return Result(
            self.key,
            "previewed",
            f"would write {path}",
            diagnostics=tuple(json.dumps(merged, indent=2).splitlines()),
        )

    def apply(self, registration: Registration, run: Runner) -> Result:
        """`run` is unused: this client is a file editor, not a subprocess."""
        path = self.resolve_path()
        before = read_raw(path)
        merged = merge_entry(load_config(path), registration.key, server_entry(registration))

        # The desktop app owns this file and writes to it too. Without this check a concurrent write
        # between our read and our replace would be silently discarded.
        if read_raw(path) != before:
            return Result(
                self.key,
                "failed",
                f"{path} changed while it was being edited — nothing was written",
                diagnostics=("Quit the Claude desktop app and try again.",),
            )

        backup = back_up(path)
        write_config(path, merged)
        return Result(
            self.key,
            "applied",
            f"wrote {path}",
            diagnostics=(f"backed up previous config to {backup}",) if backup else (),
        )
