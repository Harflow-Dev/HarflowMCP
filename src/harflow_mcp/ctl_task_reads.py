"""`harflow-ctl task-events` and `task-prompt`: direct-caller reads of one task's history.

Authority is decided in `ctl._main` (a refused caller learns nothing about which ids exist); these
handlers then resolve the id and read the record exactly as `status` does. They never import the
MCP server.
"""
from __future__ import annotations

import argparse
import json
from typing import Any

from . import event_pages, id_resolution, store, task_event_read, task_prompt_read
from .events import EVENT_KINDS
from .wire_contracts import CTL_FORMAT, envelope


def validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Usage errors, which precede every other refusal."""
    if not args.json:
        parser.error(f"{args.command} requires --json")
    if args.command == "task-prompt":
        if not 1 <= args.max_bytes <= task_prompt_read.MAX_MAX_BYTES:
            parser.error(f"--max-bytes must be 1..{task_prompt_read.MAX_MAX_BYTES}")
        return
    if not 1 <= args.limit <= event_pages.MAX_LIMIT:
        parser.error(f"--limit must be 1..{event_pages.MAX_LIMIT}")
    if args.kinds is not None:
        kinds = args.kinds.split(",")
        unknown = sorted({kind for kind in kinds if kind not in EVENT_KINDS})
        if unknown:
            parser.error("unknown event kinds: " + ", ".join(repr(kind) for kind in unknown))
        args.kinds = kinds


def _print(result: dict[str, Any], resolution: id_resolution.Resolution) -> int:
    from .ctl import CTL_JSON_VERSION
    print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": id_resolution.echo(result, primary=resolution)})))
    return 0


def _record(args: argparse.Namespace) -> tuple[id_resolution.Resolution, store.TaskRecord] | int:
    """Resolve `args.task_id` and read its record, or the exit code of the refusal already printed."""
    from . import ctl
    log_dir = ctl.default_log_dir()
    try:
        resolution, managed = ctl._resolve_task_arg(log_dir, args.task_id, guarded=True)
        # `_read_task_record` reads (and so qualifies) the record before the workflow guard.
        return resolution, ctl._read_task_record(log_dir, resolution.id, managed, include_prompt=False)
    except ctl._StatusReadRefused as exc:
        return ctl._fail(args, exc.code, exc.message, exc.data)


def cmd_task_events(args: argparse.Namespace) -> int:
    from . import ctl
    found = _record(args)
    if isinstance(found, int):
        return found
    resolution, record = found
    try:
        result = task_event_read.task_events_result(
            ctl.default_log_dir(), record, cursor=args.cursor, from_oldest=args.origin == "oldest",
            limit=args.limit, kinds=args.kinds, all_kinds=args.all_kinds)
    except (event_pages.CursorInvalid, event_pages.CursorExpired) as exc:
        code = "event_cursor_invalid" if isinstance(exc, event_pages.CursorInvalid) else "event_cursor_expired"
        return ctl._fail(args, code, f"{exc}; reload the newest page", {"reload": "newest"})
    return _print(result, resolution)


def cmd_task_prompt(args: argparse.Namespace) -> int:
    from . import ctl
    found = _record(args)
    if isinstance(found, int):
        return found
    resolution, record = found
    return _print(task_prompt_read.task_prompt_result(ctl.default_log_dir(), record, args.max_bytes), resolution)
