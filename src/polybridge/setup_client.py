"""Register the installed polybridge server with the Claude desktop app and each agent CLI found.

The reason this exists rather than a README snippet: a GUI-launched process does not inherit the
shell `PATH`, so a config that just says `"command": "polybridge-server"` fails twice over — the
executable is not found, and even when it is, the server cannot find the agent CLIs it dispatches
to. Both need absolute, resolved paths, and getting that wrong produces an error nobody can
reasonably debug.

How each client is actually written to lives in `clients/`. This module works out *what* to register
— the server's path and the PATH it needs — and reports what happened.

Three actions, mutually exclusive: install (the default), `--status` and `--uninstall`. Only install
*requires* the server binary: an uninstall has to work after it is gone, and `--status` merely looks
it up so it can say whether each entry is current. `--json` prints one versioned document instead of
the table — the Mac app reads it, so its shape is pinned by tests and changes only with `v`.
"""

from __future__ import annotations

import argparse
import json
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

# Bumped only when the `--json` document changes shape; the Mac app reads this.
JSON_VERSION = 1

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
    return _registration_for(resolve_server_command())


def find_registration() -> clients.Registration | None:
    """What install would write now, or None when the server binary is not on PATH — never raises."""
    found = shutil.which(SERVER_COMMAND)
    return _registration_for(os.path.abspath(found)) if found is not None else None


def _registration_for(server_command: str) -> clients.Registration:
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
    # Sized to the longest status present, so `not_installed` does not push its row out of line.
    status_width = max((len(result.status) for result in results), default=0)
    for result in results:
        indent = f"  {'':<{width}}  {'':<{status_width}}  "
        print(
            f"  {labels[result.client]:<{width}}  {result.status:<{status_width}}  {result.detail}"
        )
        # Only the trouble cases get their commands printed; a preview already reads as one.
        if result.status in ("failed", "unknown"):
            for step in result.steps:
                print(f"{indent}ran: {step}")
        for line in result.diagnostics:
            print(f"{indent}{line}")


def _report_status(
    inspections: list[clients.Inspection], expected: clients.Registration | None
) -> None:
    labels = {inspection.client: clients.label(inspection.client) for inspection in inspections}
    width = max((len(label) for label in labels.values()), default=0)
    for inspection in inspections:
        indent = f"  {'':<{width}}  {'':<13}  "
        if inspection.error is not None:
            state, detail = "error", inspection.error
        elif not inspection.available:
            state, detail = "unavailable", ""
        elif inspection.installed:
            current = clients.is_current(
                inspection,
                expected.command if expected else None,
                expected.path_env if expected else None,
            )
            state = "installed"
            detail = inspection.command or "(no command recorded)"
            if current is not None:
                detail += "  (current)" if current else "  (differs from what install would write)"
        else:
            state, detail = "not installed", ""
        print(f"  {labels[inspection.client]:<{width}}  {state:<13}  {detail}".rstrip())
        for line in inspection.notes:
            print(f"{indent}{line}")


def json_document(
    server_path: str | None,
    path_env: str | None,
    inspections: list[clients.Inspection],
    results: list[clients.Result] | None = None,
) -> dict:
    """The `--json` document, version `JSON_VERSION`. One row per inspected client.

    `action` is what this invocation did to that client — a `Result.status` — or null for `--status`.
    `installed`, `command` and `current` always describe the state *after* the action, from an
    inspect run once it finished. `error` is the action's own detail when it failed or is unknown,
    else the inspect's error; `notes` carries everything else worth reading.
    """
    by_client = {result.client: result for result in results or ()}
    rows = []
    for inspection in inspections:
        result = by_client.get(inspection.client)
        error = inspection.error
        notes: list[str] = []
        if result is not None:
            if result.status in ("failed", "unknown"):
                error = result.detail
                notes.extend(f"ran: {step}" for step in result.steps)
                if inspection.error is not None:
                    notes.append(f"inspect: {inspection.error}")
            else:
                notes.append(result.detail)
            notes.extend(result.diagnostics)
            follow_up = clients.follow_up(result)
            if follow_up is not None:
                notes.append(follow_up)
        notes.extend(inspection.notes)
        rows.append(
            {
                "key": inspection.client,
                "available": inspection.available,
                "installed": inspection.installed,
                "command": inspection.command,
                "current": clients.is_current(inspection, server_path, path_env),
                "action": result.status if result is not None else None,
                "error": error,
                "notes": notes,
            }
        )
    return {"v": JSON_VERSION, "server_path": server_path, "clients": rows}


def _print_json(document: dict) -> None:
    print(json.dumps(document, indent=2))


def _status(args: argparse.Namespace, selected: list[clients.Client]) -> int:
    expected = find_registration()
    inspections = clients.inspect_all(selected, SERVER_KEY)
    if args.json:
        _print_json(
            json_document(
                expected.command if expected else None,
                expected.path_env if expected else None,
                inspections,
            )
        )
    else:
        print(f"server:  {expected.command if expected else f'`{SERVER_COMMAND}` not on PATH'}\n")
        _report_status(inspections, expected)
    return clients.status_exit_code(inspections)


def _uninstall(args: argparse.Namespace, selected: list[clients.Client]) -> int:
    results = clients.unregister(selected, SERVER_KEY, named=clients.named_clients(args.client))
    if args.json:
        _print_json(json_document(None, None, clients.inspect_all(selected, SERVER_KEY), results))
    else:
        _report(results)
        for note in clients.closing_notes(results):
            print(f"\n{note}")
    return clients.uninstall_exit_code(results)


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
        help="install only: print what would be written or run, without changing anything",
    )
    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        "--status",
        action="store_true",
        help="report whether each client has polybridge registered; changes nothing",
    )
    action.add_argument(
        "--uninstall",
        action="store_true",
        help="remove polybridge from each client (opencode has to be edited by hand)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help=f"print one JSON document (version {JSON_VERSION}) instead of the table",
    )
    args = parser.parse_args(argv)

    # Rejected with --uninstall too: there is no removal preview, and a dry run that quietly
    # removed things would be the worst possible reading of the flag.
    if args.dry_run and (args.status or args.uninstall):
        parser.error("--dry-run only applies to install, not to --status or --uninstall")

    try:
        selected = _selected(args)
    except (SetupError, clients.UnknownClient) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.status:
        return _status(args, selected)
    if args.uninstall:
        return _uninstall(args, selected)
    return _install(args, selected)


def _install(args: argparse.Namespace, selected: list[clients.Client]) -> int:
    try:
        registration = build_registration()
    except SetupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        if args.json:
            # The Mac app parses stdout, so a run that got past argument checking always yields
            # the document — here with every selected client failed and nothing attempted.
            reason = str(exc).splitlines()[0]
            failed = [
                clients.Result(client.key, "failed", f"nothing was attempted: {reason}")
                for client in selected
            ]
            _print_json(
                json_document(None, None, clients.inspect_all(selected, SERVER_KEY), failed)
            )
        return 1

    # With --json, stdout carries the document and nothing else; the warnings below go to stderr.
    if not args.json:
        print(f"server:  {registration.command}")
        print(f"PATH:    {registration.path_env}\n")

    results = clients.register(
        selected,
        registration,
        dry_run=args.dry_run,
        named=clients.named_clients(args.client),
    )
    if args.json:
        _print_json(
            json_document(
                registration.command,
                registration.path_env,
                clients.inspect_all(selected, SERVER_KEY),
                results,
            )
        )
    else:
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

    if not args.json:
        for note in clients.closing_notes(results):
            print(f"\n{note}")

    return clients.exit_code(results)


if __name__ == "__main__":
    raise SystemExit(main())
