"""Register the installed polybridge server with the Claude desktop app.

The reason this exists rather than a README snippet: a GUI-launched process does not inherit the
shell `PATH`, so a config that just says `"command": "polybridge-server"` fails twice over — the
executable is not found, and even when it is, the server cannot find the agent CLIs it dispatches
to. Both need absolute, resolved paths, and getting that wrong produces an error nobody
can reasonably debug.

This edits a file it does not own, so it merges rather than replaces, backs up first, refuses to
touch a config it cannot parse, and writes atomically.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from . import backends

SERVER_KEY = "polybridge"
SERVER_COMMAND = "polybridge-server"

# Kept on PATH so the server can still reach ordinary system tools (it shells out to `git`).
BASE_PATH_DIRS = (
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin",
    "/usr/sbin",
    "/sbin",
)

MANUAL_INSTRUCTIONS = f"""
Add this to your MCP client's config by hand, using absolute paths:

  "mcpServers": {{
    "{SERVER_KEY}": {{
      "command": "<absolute path to {SERVER_COMMAND}>",
      "env": {{ "PATH": "<dir holding claude>:/usr/local/bin:/usr/bin:/bin" }}
    }}
  }}
"""


class SetupError(RuntimeError):
    """Something the user needs to fix before setup can proceed."""


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


def resolve_server_command() -> str:
    """Absolute path to the installed server, since the desktop app has no useful PATH."""
    found = shutil.which(SERVER_COMMAND)
    if found is None:
        raise SetupError(
            f"`{SERVER_COMMAND}` is not on PATH. Install it first:\n"
            "  uv tool install .\n"
            "then re-run this command."
        )
    # Absolute but NOT symlink-resolved — see build_path_env for why that matters.
    return os.path.abspath(found)


def _agent_paths() -> tuple[str | None, ...]:
    """Where each backend's CLI lives, so the spawned server can find every one of them."""
    return tuple(shutil.which(b.binary) for b in backends.BACKENDS.values())


def build_path_env(server_command: str, *dependency_paths: str | None) -> str:
    """PATH for the spawned server: where its own binary and the tools it shells out to live.

    Symlinks are deliberately left unresolved. Claude Code installs itself as
    `~/.local/bin/claude` pointing at a versioned directory, so resolving it would pin PATH to
    today's version and break dispatch the next time Claude Code updates itself.

    Directories are not filtered for existence: a missing PATH entry is ignored at exec time, and
    keeping them means a tool installed into one of these later still gets found.
    """
    candidates = [os.path.dirname(server_command)]
    candidates.extend(
        os.path.dirname(os.path.abspath(path)) for path in dependency_paths if path is not None
    )
    candidates.extend(BASE_PATH_DIRS)

    return os.pathsep.join(dict.fromkeys(candidates))


def server_entry(server_command: str, *dependency_paths: str | None) -> dict[str, Any]:
    return {
        "command": server_command,
        "env": {"PATH": build_path_env(server_command, *dependency_paths)},
    }


def read_raw(path: Path) -> str | None:
    """Current file contents, or None if absent. Used both to parse and to detect concurrent edits."""
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


def merge_entry(config: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    """Set our server key, leaving every other key in the config exactly as it was."""
    servers = config.get("mcpServers")
    if servers is not None and not isinstance(servers, dict):
        raise SetupError("existing 'mcpServers' is not a JSON object. Refusing to overwrite it.")

    merged = dict(config)
    merged["mcpServers"] = {**(servers or {}), SERVER_KEY: entry}
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
    """Replace the config atomically, preserving its permissions.

    A temp file in the same directory plus os.replace means a reader never sees a half-written
    config. The mode is copied across because NamedTemporaryFile creates 0600, which would
    otherwise silently tighten the file's permissions.
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


def is_ephemeral_install(server_command: str) -> bool:
    """Whether the command lives in a project-local virtualenv rather than a durable location.

    Happens when this is run through `uv run` from a checkout. The config would then point at a
    path that disappears with the venv, so it is worth saying so rather than writing it silently.
    """
    parts = Path(server_command).parts
    return ".venv" in parts or "site-packages" in parts


def setup(config_path: Path, *, dry_run: bool = False) -> dict[str, Any]:
    """Install our entry into `config_path`. Returns the resulting config."""
    # A symlinked config should have its target rewritten, not be replaced by a regular file.
    if config_path.is_symlink():
        config_path = Path(os.path.realpath(config_path))

    server_command = resolve_server_command()
    entry = server_entry(server_command, *_agent_paths(), shutil.which("git"))

    before = read_raw(config_path)
    merged = merge_entry(load_config(config_path), entry)

    if dry_run:
        return merged

    # The desktop app owns this file and writes to it too. Without this check a concurrent write
    # between our read and our replace would be silently discarded.
    if read_raw(config_path) != before:
        raise SetupError(
            f"{config_path} changed while it was being edited — nothing was written. "
            "Quit the Claude desktop app and try again."
        )

    backup = back_up(config_path)
    write_config(config_path, merged)
    if backup is not None:
        print(f"backed up previous config to {backup}")
    return merged


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="polybridge-setup",
        description="Register polybridge with the Claude desktop app.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="config file to edit (defaults to the Claude desktop app's)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the resulting config without writing anything",
    )
    args = parser.parse_args(argv)

    try:
        config_path = args.config or desktop_config_path()
        merged = setup(config_path, dry_run=args.dry_run)
    except SetupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    entry = merged["mcpServers"][SERVER_KEY]
    if args.dry_run:
        print(f"would write {config_path}:")
        print(json.dumps(merged, indent=2))
        return 0

    print(f"registered {SERVER_KEY} in {config_path}")
    print(f"  command: {entry['command']}")
    print(f"  PATH:    {entry['env']['PATH']}")

    if is_ephemeral_install(entry["command"]):
        print(
            f"\nwarning: that command lives in a virtualenv, which will break if it is removed.\n"
            f"Install it durably instead:\n  uv tool install .\n  {SERVER_COMMAND.replace('-server', '-setup')}",
            file=sys.stderr,
        )

    absent = [b.binary for b in backends.BACKENDS.values() if shutil.which(b.binary) is None]
    if absent:
        print(
            f"\nwarning: these agent CLIs are not on PATH, so their backends will be unusable: "
            f"{', '.join(absent)}. Install them, then re-run this command to record their location.",
            file=sys.stderr,
        )

    print("\nRestart the Claude desktop app to pick up the change.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
