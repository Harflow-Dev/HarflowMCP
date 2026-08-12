"""Client registry: key -> implementation, plus selection and orchestration.

Mirrors `backends/`: adding a client should mean one new module and one registry entry, and nothing
outside this package branches on a client's name.

Registration order is report order, and the desktop app comes first because it is the one that
needs a restart.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from .base import (
    CLI_TIMEOUT_SECONDS,
    Availability,
    Client,
    CliClient,
    Registration,
    Result,
    RunResult,
    Runner,
    SetupError,
    Status,
    run_cli,
)
from .claude_code import ClaudeCodeClient
from .codex import CodexClient
from .desktop import DesktopClient, desktop_config_path
from .opencode import OpencodeClient

ALL = "all"

CLIENTS: dict[str, Client] = {
    client.key: client
    for client in (DesktopClient(), ClaudeCodeClient(), CodexClient(), OpencodeClient())
}


class UnknownClient(ValueError):
    """A client name that is not registered."""


def get(key: str) -> Client:
    try:
        return CLIENTS[key]
    except KeyError:
        raise UnknownClient(
            f"unknown client {key!r}; expected one of {sorted(CLIENTS)} or {ALL!r}"
        ) from None


def parse_selection(values: Sequence[str] | None) -> list[str]:
    """Client keys from repeated and/or comma-separated `--client` values. Empty means every one."""
    if not values:
        return list(CLIENTS)

    keys: list[str] = []
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if not part:
                continue
            if part == ALL:
                keys.extend(CLIENTS)
                continue
            get(part)
            keys.append(part)

    if not keys:
        raise UnknownClient(f"no clients selected; expected one of {sorted(CLIENTS)} or {ALL!r}")
    return list(dict.fromkeys(keys))


def select(values: Sequence[str] | None) -> list[Client]:
    return [CLIENTS[key] for key in parse_selection(values)]


def named_clients(values: Sequence[str] | None) -> frozenset[str]:
    """The keys the user named individually — those are the ones whose absence is an error.

    Per key rather than one flag for the whole run: in `--client codex,all`, Codex *was* named, so it
    being missing is still an error, while the clients that only arrived via `all` are skipped as
    usual. A single boolean got that backwards for the mixed case.
    """
    if not values:
        return frozenset()
    return frozenset(
        part.strip()
        for value in values
        for part in value.split(",")
        if part.strip() and part.strip() != ALL
    )


def register(
    clients: Sequence[Client],
    registration: Registration,
    *,
    run: Runner = run_cli,
    dry_run: bool = False,
    named: frozenset[str] = frozenset(),
) -> list[Result]:
    """Register with each client in turn, one result each.

    Sequential on purpose: nothing measured says these CLIs lock their own config files, so two of
    them writing at once is not something to find out about in an installer.

    One client's failure never stops the others — a broken desktop config should not cost you the
    three registrations that would have worked. `named` holds the clients the user asked for by name,
    for which "not installed" is an error rather than a skip.
    """
    results: list[Result] = []
    for client in clients:
        availability = _availability(client)

        if dry_run:
            results.append(_preview(client, registration, availability))
            continue

        if not availability.available:
            asked_for = client.key in named
            results.append(
                Result(
                    client.key,
                    "failed" if asked_for or availability.errored else "skipped",
                    availability.reason or "unavailable",
                )
            )
            continue

        results.append(_apply(client, registration, run))
    return results


def override_config_path(chosen: Sequence[Client], config_path: Path) -> list[Client]:
    """Point whichever selected client edits a config file at `config_path` instead.

    Lives here rather than in the command line because "which client has a config file we own" is a
    fact about the clients — only the desktop app does; the CLIs are told, not edited. It asks for the
    `with_config_path` capability rather than naming a class, so a second editable client would work
    by implementing it.
    """
    if not any(_accepts_config_path(client) for client in chosen):
        labels = ", ".join(
            client.label for client in CLIENTS.values() if _accepts_config_path(client)
        )
        raise SetupError(f"a config path only applies to {labels}, which is not selected.")
    return [
        client.with_config_path(config_path) if _accepts_config_path(client) else client
        for client in chosen
    ]


def _accepts_config_path(client: Client) -> bool:
    return callable(getattr(client, "with_config_path", None))


def closing_notes(results: Sequence[Result]) -> list[str]:
    """What to do next, said only about clients that actually changed, grouped by the same advice."""
    notes: dict[str, list[str]] = {}
    for result in results:
        if result.status != "applied":
            continue
        client = CLIENTS.get(result.client)
        if client is None:
            continue
        notes.setdefault(client.post_apply_note, []).append(client.label)
    return [f"{', '.join(labels)}: {note}" for note, labels in notes.items()]


def label(key: str) -> str:
    """Display name for a result's client. Falls back to the key so reporting cannot raise."""
    client = CLIENTS.get(key)
    return client.label if client is not None else key


def exit_code(results: Sequence[Result]) -> int:
    """Non-zero if anything went wrong, or if nothing at all was accomplished."""
    progressed = any(result.status in ("applied", "previewed") for result in results)
    problems = any(result.status in ("failed", "unknown") for result in results)
    return 1 if problems or not progressed else 0


def _availability(client: Client) -> Availability:
    try:
        return client.availability()
    except Exception as exc:  # noqa: BLE001 — see _apply
        return Availability(False, reason=_first_line(exc), errored=True)


def _preview(client: Client, registration: Registration, availability: Availability) -> Result:
    """A preview never fails on an absent client: it says what it would run, and notes the absence."""
    try:
        result = client.preview(registration)
    except Exception as exc:  # noqa: BLE001 — see _apply
        # `failed`, not `previewed`: if the preview cannot be produced the real thing would not work
        # either, and reporting progress here would hide that behind a zero exit.
        return Result(client.key, "failed", _first_line(exc))
    if availability.available:
        return result
    return Result(
        result.client,
        result.status,
        result.detail,
        result.steps,
        (*result.diagnostics, f"note: {availability.reason}"),
    )


def _apply(client: Client, registration: Registration, run: Runner) -> Result:
    """Catch every ordinary exception.

    "One client's failure never stops the others" has to hold for the failures nobody enumerated, or
    it is a comment rather than a property. A non-UTF-8 desktop config raises UnicodeDecodeError, not
    OSError; a bug in one client raises whatever it raises. Either would otherwise cost the user the
    registrations that would have worked. `BaseException` is deliberately not caught, so
    KeyboardInterrupt and SystemExit still stop the run.
    """
    try:
        return client.apply(registration, run)
    except Exception as exc:  # noqa: BLE001
        return Result(client.key, "failed", _first_line(exc))


def _first_line(exc: BaseException) -> str:
    """Exception text is for a human reading one row of a table; `repr` when there is no message."""
    text = str(exc).strip()
    return text.splitlines()[0] if text else repr(exc)


__all__ = [
    "ALL",
    "CLIENTS",
    "CLI_TIMEOUT_SECONDS",
    "Availability",
    "ClaudeCodeClient",
    "Client",
    "CliClient",
    "CodexClient",
    "DesktopClient",
    "OpencodeClient",
    "Registration",
    "Result",
    "RunResult",
    "Runner",
    "SetupError",
    "Status",
    "UnknownClient",
    "closing_notes",
    "desktop_config_path",
    "exit_code",
    "get",
    "named_clients",
    "label",
    "override_config_path",
    "parse_selection",
    "register",
    "run_cli",
    "select",
]
