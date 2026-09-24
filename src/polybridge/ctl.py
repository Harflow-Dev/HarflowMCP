"""polybridge-ctl: a CLI over the task records `polybridge-server` writes to disk.

Deliberately separate from the MCP server: this never starts retention, never constructs a
`TaskRegistry`, and never signals a process. Its one write is `send`, which appends a message to a
live-input task's inbox under that inbox's lock (see `inbox.py`) for the owning server to deliver. It reads exactly what `store` and
`tasks.default_log_dir` already expose, which is also why `list`'s status resolution is cheap for a
settled task — see `store.resolve_status`'s `detail=False` shortcut.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from . import identity, inbox, store
from .tasks import default_log_dir

_DURATION_RE = re.compile(r"^(\d+)([smhdw])$")
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def _parse_duration(raw: str) -> timedelta:
    match = _DURATION_RE.match(raw.strip())
    if match is None:
        raise ValueError(f"invalid duration {raw!r}; expected e.g. '7d', '12h', '30m'")
    value, unit = match.groups()
    return timedelta(seconds=int(value) * _DURATION_UNITS[unit])


class _ArgumentParser(argparse.ArgumentParser):
    """Adds the `--json` error document to argparse's own usage-error path.

    `json_requested` is set by `main` from a raw scan of argv, before parsing starts — parsing is
    exactly where these errors are raised, so it cannot depend on parsing having already succeeded.
    """

    json_requested = False

    def error(self, message: str) -> None:
        if self.json_requested:
            print(json.dumps({"v": 1, "error": {"code": "usage", "message": message}}))
        print(f"{self.prog}: error: {message}", file=sys.stderr)
        raise SystemExit(2)


def _build_parser() -> tuple[_ArgumentParser, ...]:
    parser = _ArgumentParser(prog="polybridge-ctl")
    sub = parser.add_subparsers(dest="command", required=True, parser_class=_ArgumentParser)

    list_p = sub.add_parser("list", help="list tasks recorded on disk")
    list_p.add_argument(
        "--since", default=None, help="only tasks started within this long ago, e.g. 7d, 12h"
    )
    list_p.add_argument("--json", action="store_true")

    status_p = sub.add_parser("status", help="show one task's full state")
    status_p.add_argument("task_id")
    status_p.add_argument("--json", action="store_true")

    send_p = sub.add_parser(
        "send", help="queue a message for a running live-input task (reports queued, not delivered)"
    )
    send_p.add_argument("task_id")
    send_p.add_argument("text")
    send_p.add_argument("--json", action="store_true")

    return parser, list_p, status_p, send_p


def _print_table(entries: list[dict[str, Any]]) -> None:
    headers = ("task_id", "backend", "status", "started_at", "owner_pid", "repo_path")
    rows = [
        (
            entry["task_id"],
            entry["backend"],
            entry["status"],
            entry["started_at"],
            str((entry.get("owner") or {}).get("pid") or "-"),
            entry["repo_path"],
        )
        for entry in entries
    ]
    if not rows:
        print("no tasks")
        return
    widths = [max(len(headers[i]), *(len(row[i]) for row in rows)) for i in range(len(headers))]

    def fmt(row: tuple[str, ...]) -> str:
        return "  ".join(cell.ljust(width) for cell, width in zip(row, widths))

    print(fmt(headers))
    for row in rows:
        print(fmt(row))


def _cmd_list(args: argparse.Namespace, parser: _ArgumentParser) -> int:
    log_dir = default_log_dir()
    records = store.read_all(log_dir)

    if args.since is not None:
        try:
            window = _parse_duration(args.since)
        except ValueError as exc:
            parser.error(str(exc))
            return 2  # pragma: no cover - parser.error always raises SystemExit
        cutoff = datetime.now(timezone.utc) - window
        kept = []
        for record in records:
            try:
                started = datetime.fromisoformat(record.started_at)
            except ValueError:
                print(
                    f"polybridge-ctl: skipping {record.task_id}: unparsable started_at",
                    file=sys.stderr,
                )
                continue
            if started >= cutoff:
                kept.append(record)
        records = kept

    entries = [store.brief(log_dir, record) for record in records]
    if args.json:
        print(json.dumps({"v": 1, "tasks": entries}))
    else:
        _print_table(entries)
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    log_dir = default_log_dir()
    try:
        task_id = store.validate_task_id(args.task_id)
    except store.InvalidTaskId:
        message = f"not a valid task id: {args.task_id!r}"
        print(f"polybridge-ctl: error: {message}", file=sys.stderr)
        if args.json:
            print(json.dumps({"v": 1, "error": {"code": "invalid_task_id", "message": message}}))
        return 1

    record = store.read(log_dir, task_id)
    if record is None:
        message = f"unknown task_id: {task_id}"
        print(f"polybridge-ctl: error: {message}", file=sys.stderr)
        if args.json:
            print(json.dumps({"v": 1, "error": {"code": "unknown_task", "message": message}}))
        return 1

    snapshot = store.snapshot(log_dir, record)
    if args.json:
        print(json.dumps({"v": 1, "task": snapshot}))
    else:
        for key, value in snapshot.items():
            print(f"{key}: {value}")
    return 0


def _cmd_send(args: argparse.Namespace) -> int:
    def fail(code: str, message: str) -> int:
        print(f"polybridge-ctl: error: {message}", file=sys.stderr)
        if args.json:
            print(json.dumps({"v": 1, "error": {"code": code, "message": message}}))
        return 1

    try:
        task_id = store.validate_task_id(args.task_id)
    except store.InvalidTaskId:
        return fail("invalid_task_id", f"not a valid task id: {args.task_id!r}")
    if not args.text.strip():
        return fail("empty_text", "text must be a non-empty string")

    try:
        by = identity.own_identity()
    except Exception:
        by = None
    try:
        result = inbox.send_to_record(default_log_dir(), task_id, args.text, by=by)
    except inbox.SendRefused as exc:
        return fail(exc.code, str(exc))

    if args.json:
        print(json.dumps({"v": 1, "result": result}))
    else:
        print(f"queued {result['message_id']} for {task_id} (not yet delivered)")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Entry point for the `polybridge-ctl` console script, which calls `sys.exit(main())`."""
    argv = sys.argv[1:] if argv is None else list(argv)
    json_requested = "--json" in argv

    parser, list_p, status_p, send_p = _build_parser()
    for each in (parser, list_p, status_p, send_p):
        each.json_requested = json_requested

    args = parser.parse_args(argv)

    if args.command == "list":
        return _cmd_list(args, list_p)
    if args.command == "send":
        return _cmd_send(args)
    return _cmd_status(args)


if __name__ == "__main__":
    sys.exit(main())
