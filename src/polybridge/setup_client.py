"""Register the installed polybridge server with the Claude desktop app and each agent CLI found.

The reason this exists rather than a README snippet: a GUI-launched process does not inherit the
shell `PATH`, so a config that just says `"command": "polybridge-server"` fails twice over — the
executable is not found, and even when it is, the server cannot find the agent CLIs it dispatches
to. Both need absolute, resolved paths, and getting that wrong produces an error nobody can
reasonably debug.

How each client is actually written to lives in `clients/`. This module works out *what* to register
— the server's path and the PATH it needs — and reports what happened.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

from . import backends, clients

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

SetupError = clients.SetupError


def resolve_server_command() -> str:
    """Absolute path to the installed server, since a GUI client has no useful PATH."""
    found = shutil.which(SERVER_COMMAND)
    if found is None:
        raise SetupError(
            f"`{SERVER_COMMAND}` is not on PATH. Install it first:\n"
            "  uv tool install . --force --no-cache\n"
            "then re-run this command."
        )
    # Absolute but NOT symlink-resolved — see build_path_env for why that matters.
    return os.path.abspath(found)


def _agent_paths() -> tuple[str | None, ...]:
    """Where each backend's CLI lives, so the spawned server can find every one of them."""
    return tuple(shutil.which(backend.binary) for backend in backends.BACKENDS.values())


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


def build_registration() -> clients.Registration:
    server_command = resolve_server_command()
    path_env = build_path_env(server_command, *_agent_paths(), shutil.which("git"))
    return clients.Registration(key=SERVER_KEY, command=server_command, path_env=path_env)


def is_ephemeral_install(server_command: str) -> bool:
    """Whether the command lives in a project-local virtualenv rather than a durable location.

    Happens when this is run through `uv run` from a checkout. The config would then point at a
    path that disappears with the venv, so it is worth saying so rather than writing it silently.
    """
    parts = Path(server_command).parts
    return ".venv" in parts or "site-packages" in parts


def _selected(args: argparse.Namespace) -> list[clients.Client]:
    """The clients to register with, honouring a `--desktop-config` override."""
    chosen = clients.select(args.client)
    if args.desktop_config is None:
        return chosen
    return clients.override_config_path(chosen, args.desktop_config)


def _report(results: list[clients.Result]) -> None:
    labels = {result.client: clients.label(result.client) for result in results}
    width = max((len(label) for label in labels.values()), default=0)
    for result in results:
        indent = f"  {'':<{width}}  {'':<9}  "
        print(f"  {labels[result.client]:<{width}}  {result.status:<9}  {result.detail}")
        # Only the trouble cases get their commands printed; a preview already reads as one.
        if result.status in ("failed", "unknown"):
            for step in result.steps:
                print(f"{indent}ran: {step}")
        for line in result.diagnostics:
            print(f"{indent}{line}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="polybridge-setup",
        description="Register polybridge with the Claude desktop app and each agent CLI found.",
    )
    parser.add_argument(
        "--client",
        action="append",
        metavar="NAME",
        help=(
            "which client to register with; repeatable or comma-separated. "
            f"One of {', '.join(clients.CLIENTS)}, or '{clients.ALL}'. "
            "Defaults to the desktop app plus each agent CLI found."
        ),
    )
    parser.add_argument(
        "--desktop-config",
        type=Path,
        default=None,
        help="config file for the Claude desktop app (defaults to its usual location)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print what would be written or run, without changing anything",
    )
    args = parser.parse_args(argv)

    try:
        selected = _selected(args)
        registration = build_registration()
    except (SetupError, clients.UnknownClient) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"server:  {registration.command}")
    print(f"PATH:    {registration.path_env}\n")

    results = clients.register(
        selected,
        registration,
        dry_run=args.dry_run,
        named=clients.named_clients(args.client),
    )
    _report(results)

    if is_ephemeral_install(registration.command):
        print(
            "\nwarning: that command lives in a virtualenv, which will break if it is removed.\n"
            "Install it durably instead:\n"
            "  uv tool install . --force --no-cache\n"
            f"  {SERVER_COMMAND.replace('-server', '-setup')}",
            file=sys.stderr,
        )

    absent = [
        backend.binary
        for backend in backends.BACKENDS.values()
        if shutil.which(backend.binary) is None
    ]
    if absent:
        print(
            f"\nwarning: these agent CLIs are not on PATH, so their backends will be unusable: "
            f"{', '.join(absent)}. Install them, then re-run this command to record their location.",
            file=sys.stderr,
        )

    for note in clients.closing_notes(results):
        print(f"\n{note}")

    return clients.exit_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
