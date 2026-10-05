"""polybridge-ctl: a CLI over the task records `polybridge-server` writes to disk.

Deliberately separate from the MCP server, and it never starts retention. `list` and `status` only
read what `store` and `tasks.default_log_dir` already expose (which is also why `list`'s status
resolution is cheap for a settled task — see `store.resolve_status`'s `detail=False` shortcut), and
never construct a `TaskRegistry`. `send` appends a message to a live-input task's inbox under that
inbox's lock (see `inbox.py`) for the owning server to deliver.

The control commands act (A4.2): `cancel` runs the same cascade as `cancel_task` from a registry
this process owns; `takeover` / `takeover-attach` are the human-only takeover (`takeover.py`); `run`
and `resume` fork a process that owns the new task until it settles (`detached.py`). `backends`
reports the registered backends and whether each binary is on PATH (`backends.is_installed`) — no
`--version` probe, no subprocess. Every command prints one versioned JSON document
(`"v": 5`, `CTL_JSON_VERSION`) with `--json`. v5 adds Run workflow node presentation
metadata: parent/root run links, orchestrator mode and session owner, input source,
tree settling and suspended-via-root flags, per-execution child invocation status,
and workflow-validate dependency reports.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shlex
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import backends, control, detached, identity, inbox, store, takeover
from . import tasks as tasks_module
from .tasks import default_log_dir

# Bumped to 2 when `status`/`list`'s snapshot/brief documents gained `resume_command` (Monitor
# piece 3/3). `setup` and the event log are separate contracts and stay at v1 — see CLAUDE.md's
# "The Monitor app is a consumer of three frozen contracts".
CTL_JSON_VERSION = 5

# How long `cancel`/`takeover` stay alive for a failing `.sig` write to be retried before exiting —
# the lease (60 s) is what a later recovery waits for anyway.
PHASE_WRITE_SETTLE_SECONDS = 60.0

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
            print(json.dumps({"v": CTL_JSON_VERSION, "error": {"code": "usage", "message": message}}))
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
    page_p = sub.add_parser('task-list-page', help='bounded task header history')
    page_p.add_argument('--json', action='store_true')
    page_p.add_argument('--limit', type=int, default=100)
    page_p.add_argument('--cursor')
    page_p.add_argument('--active-only', action='store_true')
    page_p.add_argument('--session-id')
    page_p.add_argument('--task-ids')

    backends_p = sub.add_parser(
        "backends", help="list the registered backends and whether each is on PATH"
    )
    backends_p.add_argument("--json", action="store_true")

    status_p = sub.add_parser("status", help="show one task's full state")
    status_p.add_argument("task_id")
    status_p.add_argument("--json", action="store_true")

    send_p = sub.add_parser(
        "send", help="queue a message for a running live-input task (reports queued, not delivered)"
    )
    send_p.add_argument("task_id")
    send_p.add_argument("text")
    send_p.add_argument("--json", action="store_true")

    cancel_p = sub.add_parser(
        "cancel", help="cancel a task and, best-effort, its live descendants (like cancel_task)"
    )
    cancel_p.add_argument("task_id")
    cancel_p.add_argument("--json", action="store_true")

    takeover_p = sub.add_parser(
        "takeover",
        help="stop a task's headless run and print the command that resumes its session "
        "interactively (for a person, never an agent)",
    )
    takeover_p.add_argument("task_id")
    takeover_p.add_argument("--json", action="store_true")

    attach_p = sub.add_parser(
        "takeover-attach", help="record the interactive terminal's process for a takeover"
    )
    attach_p.add_argument("task_id")
    attach_p.add_argument("--pid", type=int, required=True)
    attach_p.add_argument("--json", action="store_true")

    run_p = sub.add_parser("run", help="start a task, owned by a detached process until it settles")
    run_p.add_argument("--backend", default=None)
    run_p.add_argument("--workflow", default=None)
    run_p.add_argument("--monitor", action="store_true", help="Human Monitor-owned workflow interaction")
    run_p.add_argument("--repo", required=True)
    run_p.add_argument("--freedom", default=None)
    run_p.add_argument("--prompt", required=True)
    run_p.add_argument("--model", default=None)
    run_p.add_argument("--max-turns", type=int, default=None)
    run_p.add_argument("--reasoning-effort", default=None)
    run_p.add_argument("--network", choices=("true", "false"), default=None)
    run_p.add_argument("--group", default=None)
    run_p.add_argument("--title", default=None)
    run_p.add_argument("--json", action="store_true")

    resume_p = sub.add_parser(
        "resume", help="continue a finished task's session, owned by a detached process"
    )
    resume_p.add_argument("task_id")
    resume_p.add_argument("text")
    resume_p.add_argument("--max-turns", type=int, default=None)
    resume_p.add_argument("--network", choices=("true", "false"), default=None)
    resume_p.add_argument("--json", action="store_true")

    allowlist_p = sub.add_parser("mcp-allowlist", help="inspect or explicitly edit harness-global MCP approval rules")
    allowlist_p.add_argument("--backend", required=True, choices=tuple(backends.BACKENDS))
    allowlist_p.add_argument("--json", action="store_true")
    edits = allowlist_p.add_mutually_exclusive_group()
    edits.add_argument("--allow")
    edits.add_argument("--remove")

    workflow_parsers = []
    for action in ("validate", "list", "list-page", "get", "save", "delete", "build", "start", "list-runs", "status", "detail", "wait", "pause", "resume", "cancel", "builder-followup", "builder-apply", "inspect", "recover", "migrate", "abandon-dispatch"):
        wp = sub.add_parser("workflow-" + action, help=action + " workflows")
        wp.add_argument("--json", action="store_true")
        if action in {"get", "save", "delete", "build", "start"}:
            wp.add_argument("name")
        if action in {"status", "detail", "wait", "pause", "resume", "cancel", "builder-followup", "inspect", "recover", "abandon-dispatch"}:
            wp.add_argument("workflow_run_id")
        if action == "list":
            wp.add_argument("--monitor-view", action="store_true")
        if action == "status":
            wp.add_argument("--monitor-view", action="store_true")
            wp.add_argument("--snapshot", action="store_true")
            wp.add_argument("--cursor")
        if action == "detail":
            wp.add_argument("--monitor-view", action="store_true")
            wp.add_argument("--view", required=True)
            wp.add_argument("--cursor")
        if action == "abandon-dispatch":
            wp.add_argument("execution_id")
            wp.add_argument("task_id")
            wp.add_argument("--reason", required=True)
            wp.add_argument("--confirm-no-process", action="store_true", required=True)
        if action == "inspect":
            wp.add_argument("execution_id")
            wp.add_argument("--task-id")
            wp.add_argument("--view", choices=("result", "activity"), default="result")
            wp.add_argument("--cursor")
            wp.add_argument("--limit", type=int)
            wp.add_argument("--before-seq", type=int)
            wp.add_argument("--after-seq", type=int)
        if action == "list-runs":
            wp.add_argument("--offset", type=int, default=0)
            wp.add_argument("--limit", type=int, default=100)
        if action == 'list-page':
            wp.add_argument('--limit', type=int, default=100)
            wp.add_argument('--cursor')
            wp.add_argument('--active-only', action='store_true')
            wp.add_argument('--related-run-id')
            wp.add_argument('--run-ids')
        if action == "recover":
            reason_group = wp.add_mutually_exclusive_group(required=True)
            reason_group.add_argument("--reason")
            reason_group.add_argument("--reason-file", help="UTF-8 recovery reason file")
            wp.add_argument("--additional-attempts", type=int, default=0)
        if action in {"start", "pause", "resume", "recover", "cancel"}:
            wp.add_argument("--monitor", action="store_true", help="Human Monitor-owned workflow interaction")
        if action == "resume":
            wp.add_argument("--decision-id")
        if action == "builder-followup":
            prompt_group = wp.add_mutually_exclusive_group(required=True)
            prompt_group.add_argument("--prompt")
            prompt_group.add_argument("--prompt-file", help="UTF-8 prompt file")
        if action == "builder-apply":
            wp.add_argument("--definition", required=True, help="Current draft JSON file, or - for stdin")
            wp.add_argument("--expected-draft-revision", type=int, required=True)
        if action in {"save", "validate"}:
            wp.add_argument("--definition", required=True, help="JSON file, or - for stdin")
            if action == "save":
                wp.add_argument("--expected-revision", type=int)
        if action in {"start", "build"}:
            wp.add_argument("--repo", required=action == "start")
            prompt_group = wp.add_mutually_exclusive_group(required=True)
            prompt_group.add_argument("--prompt")
            prompt_group.add_argument("--prompt-file", help="UTF-8 prompt file")
            wp.add_argument("--backend", required=action == "build")
            wp.add_argument("--model")
            wp.add_argument("--reasoning-effort")
            wp.add_argument("--max-turns", type=int)
            if action == "build":
                wp.add_argument("--fallbacks", default="[]", help="JSON array of ordered agent candidates")
                input_group = wp.add_mutually_exclusive_group()
                input_group.add_argument("--definition", help="Current canvas JSON file, or - for stdin")
                input_group.add_argument("--definition-json", help="Current canvas JSON object")
                source_group = wp.add_mutually_exclusive_group()
                source_group.add_argument("--source", help="JSON saved name/revision/baseline metadata")
                source_group.add_argument("--source-file", help="Saved name/revision/baseline metadata JSON file")
            else:
                wp.add_argument("--freedom", default=None, choices=("read_only", "write_in_repo", "publish", "unrestricted"))
                wp.add_argument("--network", choices=("true", "false"))
        if action == "wait":
            wp.add_argument("--timeout-seconds", type=int, default=30)
        if action == "resume":
            instructions_group = wp.add_mutually_exclusive_group()
            instructions_group.add_argument("--instructions")
            instructions_group.add_argument("--instructions-file", help="UTF-8 resume instructions file")
            wp.add_argument("--additional-attempts", type=int, default=0)
        workflow_parsers.append(wp)

    return (
        parser,
        list_p,
        backends_p,
        status_p,
        send_p,
        cancel_p,
        takeover_p,
        attach_p,
        run_p,
        resume_p,
        allowlist_p,
        *workflow_parsers,
    )


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

    from .workflow_inspection import decorate_tasks
    entries = decorate_tasks([store.brief(log_dir, record) for record in records], log_dir)
    from .workflow_inspection import filter_task_reads, managed_reader
    try:
        entries = filter_task_reads(entries, managed_reader(log_dir))
    except Exception as exc:
        return _fail(args, "workflow_read_refused", str(exc))
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "tasks": entries}))
    else:
        _print_table(entries)
    return 0


def _cmd_backends(args: argparse.Namespace) -> int:
    entries = [
        {
            "backend": backend.name,
            "binary": backend.binary,
            "installed": backends.is_installed(backend),
        }
        for backend in backends.BACKENDS.values()
    ]
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "backends": entries}))
    else:
        for entry in entries:
            state = "installed" if entry["installed"] else "not found on PATH"
            print(f"{entry['backend']}  {state}")
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    log_dir = default_log_dir()
    try:
        task_id = store.validate_task_id(args.task_id)
    except store.InvalidTaskId:
        message = f"not a valid task id: {args.task_id!r}"
        print(f"polybridge-ctl: error: {message}", file=sys.stderr)
        if args.json:
            print(json.dumps({"v": CTL_JSON_VERSION, "error": {"code": "invalid_task_id", "message": message}}))
        return 1

    from .workflow_inspection import guard_task_read, managed_reader
    try:
        guard_task_read(task_id, managed_reader(log_dir))
    except Exception as exc:
        return _fail(args, "workflow_read_refused", str(exc))

    record = store.read(log_dir, task_id)
    if record is None:
        message = f"unknown task_id: {task_id}"
        print(f"polybridge-ctl: error: {message}", file=sys.stderr)
        if args.json:
            print(json.dumps({"v": CTL_JSON_VERSION, "error": {"code": "unknown_task", "message": message}}))
        return 1

    from .workflow_inspection import decorate_tasks
    snapshot = decorate_tasks([store.snapshot(log_dir, record)], log_dir)[0]
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "task": snapshot}))
    else:
        for key, value in snapshot.items():
            print(f"{key}: {value}")
    return 0


def _cmd_send(args: argparse.Namespace) -> int:
    def fail(code: str, message: str) -> int:
        print(f"polybridge-ctl: error: {message}", file=sys.stderr)
        if args.json:
            print(json.dumps({"v": CTL_JSON_VERSION, "error": {"code": code, "message": message}}))
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
        from . import lineage
        from .workflow_hooks import refuse_message_caller
        detection = lineage.detect_caller_detail(default_log_dir())
        if detection.undecidable is not None or detection.caller is None and os.environ.get(lineage.ENV_TASK_ID):
            raise inbox.SendRefused("Caller authority cannot be verified", code="caller_undecidable")
        if detection.caller is not None:
            refuse_message_caller(default_log_dir(), detection.caller.record.task_id)
        result = inbox.send_to_record(default_log_dir(), task_id, args.text, by=by)
    except inbox.SendRefused as exc:
        return fail(exc.code, str(exc))

    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "result": result}))
    else:
        print(f"queued {result['message_id']} for {task_id} (not yet delivered)")
    return 0


def _fail(args: argparse.Namespace, code: str, message: str) -> int:
    print(f"polybridge-ctl: error: {message}", file=sys.stderr)
    if getattr(args, "json", False):
        print(json.dumps({"v": CTL_JSON_VERSION, "error": {"code": code, "message": message}}))
    return 1


def _cmd_cancel(args: argparse.Namespace) -> int:
    try:
        task_id = store.validate_task_id(args.task_id)
    except store.InvalidTaskId:
        return _fail(args, "invalid_task_id", f"not a valid task id: {args.task_id!r}")
    log_dir = default_log_dir()
    if store.read(log_dir, task_id) is None:
        return _fail(args, "unknown_task", f"unknown task_id: {task_id}")

    registry = tasks_module.TaskRegistry(log_dir=log_dir, open_monitor=False)

    async def cancel_and_settle() -> tuple[dict[str, Any], list[str]]:
        cascade = await registry.cancel_cascade(task_id)
        return cascade, await registry.settle_phase_writes(PHASE_WRITE_SETTLE_SECONDS)

    try:
        cascade, unrecorded = asyncio.run(cancel_and_settle())
    except (backends.UnsupportedCapability, ValueError) as exc:
        return _fail(args, "invalid_params", str(exc))
    except control.PhaseWriteError as exc:
        return _fail(
            args,
            "phase_write_failed",
            f"the cancel was not attempted because its phase file could not be written, so "
            f"nothing was signalled: {exc}",
        )
    record = store.read(log_dir, task_id)
    status = store.brief(log_dir, record)["status"] if record is not None else None
    result = {"task_id": task_id, "status": status, "cascade": cascade}
    if unrecorded:
        # Delivered, but the phase file saying so never landed before this process had to exit;
        # lease recovery may later record the attempt as failed.
        result["unrecorded_phase_writes"] = unrecorded
        print(
            "polybridge-ctl: warning: signals were delivered but could not all be recorded: "
            + "; ".join(unrecorded),
            file=sys.stderr,
        )
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "result": result}))
    else:
        print(f"{task_id}: {status}")
    return 0


def _cmd_takeover(args: argparse.Namespace) -> int:
    log_dir = default_log_dir()
    try:
        result = asyncio.run(
            takeover.take_over(
                log_dir,
                args.task_id,
                registry_factory=lambda: tasks_module.TaskRegistry(
                    log_dir=log_dir, open_monitor=False
                ),
            )
        )
    except control.TakeoverRefused as exc:
        return _fail(args, exc.code, str(exc))
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "result": result}))
    else:
        print(f"cd {shlex.quote(result['cwd'])}")
        print(shlex.join(result["argv"]))
        print(result["note"], file=sys.stderr)
    return 0


def _cmd_takeover_attach(args: argparse.Namespace) -> int:
    try:
        result = takeover.attach(default_log_dir(), args.task_id, args.pid)
    except control.TakeoverRefused as exc:
        return _fail(args, exc.code, str(exc))
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "result": result}))
    else:
        print(f"attached pid {result['pid']} to the takeover of {result['task_id']}")
    return 0


def _network(raw: str | None) -> bool | None:
    return None if raw is None else raw == "true"


def _run_action(args: argparse.Namespace):
    async def action(registry: Any) -> Any:
        # The MCP tool's own checks, so `run` accepts exactly what `start_task` accepts.
        from mcp import MCPError
        from mcp.types import INVALID_PARAMS

        from . import server
        from .backends import DEFAULT_FREEDOM

        if not args.prompt.strip():
            raise MCPError(INVALID_PARAMS, "prompt must be a non-empty string")
        freedom = args.freedom or DEFAULT_FREEDOM
        network = _network(args.network)
        chosen = server._backend(args.backend)
        server._check_freedom(freedom)
        server._check_turn_cap(chosen, args.max_turns)
        server._check_reasoning_effort(chosen, args.reasoning_effort)
        server._check_model(chosen, args.model)
        server._check_network(chosen, freedom, network)
        server._check_group(args.group)
        title = server._normalize_title(args.title)
        path = await server._validate_repo_path(args.repo)
        return await registry.start(
            args.prompt,
            path,
            backend=chosen,
            freedom=freedom,
            model=args.model,
            max_turns=args.max_turns,
            reasoning_effort=args.reasoning_effort,
            network=network,
            group=args.group,
            title=title,
        )

    return action


def _resume_action(args: argparse.Namespace):
    async def action(registry: Any) -> Any:
        from mcp import MCPError
        from mcp.types import INVALID_PARAMS

        from . import backends, server

        try:
            task_id = store.validate_task_id(args.task_id)
        except store.InvalidTaskId as exc:
            raise MCPError(INVALID_PARAMS, str(exc)) from None
        if not args.text.strip():
            raise MCPError(INVALID_PARAMS, "text must be a non-empty string")
        record = await asyncio.to_thread(store.read, registry.log_dir, task_id)
        if record is None:
            raise MCPError(INVALID_PARAMS, f"unknown task_id: {task_id}")
        # The same gate `resume_task` applies to a task this process did not start.
        if await asyncio.to_thread(store.record_process_alive, record):
            raise MCPError(
                INVALID_PARAMS,
                f"task {task_id} is still running; wait for it or cancel it before resuming",
            )
        network = _network(args.network)
        server._check_turn_cap(backends.get(record.backend), args.max_turns)
        server._check_network(backends.get(record.backend), record.freedom, network)
        return await registry.resume_record(
            record, args.text, max_turns=args.max_turns, network=network
        )

    return action


def _ctl_log_path() -> Path:
    return default_log_dir().parent / "ctl.log"


def _cmd_detached(args: argparse.Namespace, action: Any) -> int:
    log_dir = default_log_dir()
    outcome = detached.run_detached(
        action,
        log_path=_ctl_log_path(),
        # No retention and no open-app: the app drives these commands itself.
        registry_factory=lambda: tasks_module.TaskRegistry(log_dir=log_dir, open_monitor=False),
    )
    if outcome.kind == "task":
        if args.json:
            print(json.dumps({"v": CTL_JSON_VERSION, "result": outcome.payload}))
        else:
            print(outcome.payload["task_id"])
        return 0
    if outcome.kind == "error":
        return _fail(
            args,
            str(outcome.payload.get("code", "start_failed")),
            str(outcome.payload.get("message", "")),
        )
    print(f"polybridge-ctl: {outcome.payload['message']}", file=sys.stderr)
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "unknown": outcome.payload}))
    return 3


def _workflow_prompt(args: argparse.Namespace) -> str:
    """Read disposable prompt transport without changing its newline bytes."""
    return _workflow_text(args, "prompt")


def _workflow_text(args: argparse.Namespace, field: str) -> str | None:
    """Read exact UTF-8 control text without putting it on the argument vector."""
    if path := getattr(args, field + "_file", None):
        with Path(path).open(encoding="utf-8", newline="") as stream:
            return stream.read()
    return getattr(args, field)


def _cmd_workflow(args: argparse.Namespace) -> int:
    action = args.command.removeprefix("workflow-")
    async def invoke() -> Any:
        if action == "validate":
            from .workflows import WorkflowError, validate_definition
            raw = sys.stdin.read() if args.definition == "-" else Path(args.definition).read_text()
            value = json.loads(raw)
            try:
                canonical = validate_definition(value)
            except WorkflowError as exc:
                return {"valid": False, "error": str(exc)}
            dependencies: dict[str, Any] | None = None
            error: str | None = None
            if any(node.get("type") == "workflow" for node in canonical.get("nodes", [])):
                # Resolution is additive: it reports the pinned tree or names the problem.
                from .workflow_references import DependencyError, resolve_dependencies
                from .workflows import WorkflowStore
                try:
                    tree = resolve_dependencies(WorkflowStore(), definition=canonical, substitute_name=canonical.get("name"))
                    dependencies = {"root_workflow_id": tree["root_workflow_id"], "workflows": {ident: {"name": entry["name"], "revision": entry["revision"], "definition_sha256": entry["definition_sha256"]} for ident, entry in tree["workflows"].items()}, "edges": tree["edges"], "access": tree["access"]}
                except DependencyError as exc:
                    error = str(exc)
            return {"valid": error is None, "definition": canonical, **({"dependencies": dependencies} if dependencies is not None else {}), **({"error": error} if error is not None else {})}
        from . import server
        if action == "abandon-dispatch":
            from .workflows import WorkflowStore
            from .takeover import caller_refusal
            refusal = await asyncio.to_thread(caller_refusal, server._reg().log_dir)
            if refusal is not None:
                raise ValueError("Only a verified human may abandon an uncertain dispatch: " + refusal[1])
            return await asyncio.to_thread(WorkflowStore().abandon_dispatch, args.workflow_run_id, args.execution_id, args.task_id, args.reason, args.confirm_no_process)
        if action == "inspect":
            return await server.inspect_workflow_node(args.workflow_run_id, args.execution_id, args.task_id, args.view, args.cursor, args.limit, args.before_seq, args.after_seq)
        if action == "recover":
            reason = _workflow_text(args, "reason")
            if args.monitor:
                if not reason.strip():
                    raise ValueError("Recovery reason must be nonempty")
                return await server._workflow_call("recover", run_id=args.workflow_run_id, instructions=reason, additional_attempts=args.additional_attempts, interaction_owner="monitor")
            return await server._workflow_call("recover", run_id=args.workflow_run_id, instructions=reason, additional_attempts=args.additional_attempts)
        if action == "migrate":
            from .workflows import migrate_workflows
            from .workflow_hooks import refuse_managed
            caller = await server._verified_workflow_caller()
            if caller is not None:
                refuse_managed(server._reg().log_dir, caller.record.task_id)
            return migrate_workflows()
        if action == "builder-followup":
            return await server.followup_workflow_builder(args.workflow_run_id, _workflow_prompt(args))
        if action == "builder-apply":
            raw = sys.stdin.read() if args.definition == "-" else Path(args.definition).read_text()
            return await server.apply_workflow_draft(json.loads(raw), args.expected_draft_revision)
        if action == 'list-page':
            return await server.list_workflow_run_page(args.limit, args.cursor, args.active_only, args.related_run_id, args.run_ids.split(',') if args.run_ids is not None else None)
        if action in {"list", "list-runs"}:
            entries = await server._workflow_call(action.replace("-", "_"), **({"_bounded_read": True} if action == "list" and args.monitor_view else {}))
            if action == "list-runs":
                from .workflow_responses import history_page
                return history_page(entries, args.offset, args.limit)
            return {"workflows" if action == "list" else "runs": entries}
        if action in {"get", "delete"}:
            return await server._workflow_call(action, name=args.name)
        if action == "save":
            raw = sys.stdin.read() if args.definition == "-" else Path(args.definition).read_text()
            return await server.save_workflow(args.name, json.loads(raw), args.expected_revision)
        if action in {"start", "build"}:
            prompt = _workflow_prompt(args)
            candidate = {k: v for k, v in {"backend": args.backend, "model": args.model,
                         "reasoning_effort": args.reasoning_effort, "max_turns": args.max_turns}.items() if v is not None}
            if action == "build":
                definition = json.loads(args.definition_json) if args.definition_json else None
                if args.definition:
                    definition = json.loads(sys.stdin.read() if args.definition == "-" else Path(args.definition).read_text())
                source = json.loads(args.source) if args.source else None
                if args.source_file:
                    source = json.loads(Path(args.source_file).read_text())
                return await server.workflow_builder(args.name, prompt, args.repo, candidate, json.loads(args.fallbacks), definition, source)
            if args.monitor:
                return await server._workflow_call("start", name=args.name, prompt=prompt, repo_path=args.repo, overrides=candidate or None, freedom=args.freedom, network=_network(args.network), interaction_owner="monitor")
            return await server._workflow_call("start", name=args.name, prompt=prompt, repo_path=args.repo, overrides=candidate or None, freedom=args.freedom, network=_network(args.network))
        if action == "detail":
            if args.monitor_view:
                from .workflow_responses import monitor_detail
                if await server._bounded_workflow_caller() is not None:
                    raise ValueError("Monitor detail snapshots are only available to the local Monitor")
                run = None if args.cursor else await server._workflow_call("status", run_id=args.workflow_run_id, _bounded_read=True)
                return monitor_detail(run, args.workflow_run_id, args.view, default_log_dir().parent / "monitor_snapshots", args.cursor)
            return await server.get_workflow_run_detail(args.workflow_run_id, args.view, args.cursor)
        if action == "status" and args.monitor_view:
            from .workflow_responses import monitor, monitor_snapshot
            if args.snapshot:
                if await server._bounded_workflow_caller() is not None:
                    raise ValueError("Monitor snapshots are only available to the local Monitor")
                run = None if args.cursor else await server._workflow_call("status", run_id=args.workflow_run_id, _bounded_read=True)
                return monitor_snapshot(run, args.workflow_run_id, default_log_dir().parent / "monitor_snapshots", args.cursor)
            return monitor(await server._workflow_call("status", run_id=args.workflow_run_id, _bounded_read=True))
        if action == "wait":
            return await server._wait_workflow_full(args.workflow_run_id, args.timeout_seconds)
        if action == "resume":
            instructions = _workflow_text(args, "instructions")
            if args.monitor:
                return await server._workflow_call("resume", run_id=args.workflow_run_id, instructions=instructions, additional_attempts=args.additional_attempts, decision_id=args.decision_id, interaction_owner="monitor")
            return await server._workflow_call("resume", run_id=args.workflow_run_id, instructions=instructions, additional_attempts=args.additional_attempts, decision_id=args.decision_id)
        return await server._workflow_call(action, run_id=args.workflow_run_id, **({"interaction_owner": "monitor"} if getattr(args, "monitor", False) and action != "cancel" else {}))
    try:
        result = asyncio.run(invoke())
    except Exception as exc:
        return _fail(args, "workflow_error", str(exc))
    if args.json:
        print(json.dumps({"v": CTL_JSON_VERSION, "result": result}))
    else:
        print(json.dumps(result, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    """Entry point for the `polybridge-ctl` console script, which calls `sys.exit(main())`."""
    argv = sys.argv[1:] if argv is None else list(argv)
    json_requested = "--json" in argv

    parsers = _build_parser()
    for each in parsers:
        each.json_requested = json_requested
    parser, list_p = parsers[0], parsers[1]

    args = parser.parse_args(argv)

    if args.command == "mcp-allowlist":
        from . import mcp_allowlist, server
        try:
            if args.allow is not None or args.remove is not None:
                from .takeover import caller_refusal
                refusal = caller_refusal(server._reg().log_dir)
                if refusal is not None:
                    return _fail(args, "human_only", "Only a verified human may change global MCP approval rules: " + refusal[1])
            result = mcp_allowlist.edit(args.backend, allow=args.allow, remove=args.remove)
            print(json.dumps({"v": CTL_JSON_VERSION, "result": result}) if args.json else result["detail"])
            return 0
        except (ValueError, OSError, TypeError, AttributeError) as exc:
            return _fail(args, "allowlist_error", str(exc))
    if args.command.startswith("workflow-"):
        return _cmd_workflow(args)
    if args.command == "list":
        return _cmd_list(args, list_p)
    if args.command == 'task-list-page':
        try:
            from .workflow_inspection import filter_task_reads, managed_page_reader
            ready, managed = managed_page_reader(default_log_dir())
            if not ready:
                from .workflow_inspection import page_indexing_response
                result = page_indexing_response(default_log_dir())
                print(json.dumps({'v': CTL_JSON_VERSION, 'result': result}) if args.json else json.dumps(result, indent=2))
                return 0
            result = store.list_page(default_log_dir(), limit=args.limit, cursor=args.cursor, active_only=args.active_only, session_id=args.session_id, task_ids=args.task_ids.split(',') if args.task_ids is not None else None)
            result['items'] = filter_task_reads(result['items'], managed)
            result['related_headers'] = filter_task_reads(result.get('related_headers', []), managed)
            if managed is not None:
                result['total_active_count'] = sum(item.get('status') not in store.TERMINAL_RECORD_STATUSES for item in result['items'])
                result.pop('total_active_root_count', None)
                result.pop('total_attention_root_count', None)
                result.update(next_cursor=None, has_more=False)
            print(json.dumps({'v': CTL_JSON_VERSION, 'result': result}) if args.json else json.dumps(result, indent=2))
            return 0
        except (ValueError, OSError) as exc:
            return _fail(args, 'invalid_params', str(exc))
    if args.command == "backends":
        return _cmd_backends(args)
    if args.command == "send":
        return _cmd_send(args)
    if args.command == "cancel":
        return _cmd_cancel(args)
    if args.command == "takeover":
        return _cmd_takeover(args)
    if args.command == "takeover-attach":
        return _cmd_takeover_attach(args)
    if args.command == "run":
        if args.workflow:
            if args.group is not None or args.title is not None:
                return _fail(args, "invalid_params", "workflow runs do not accept task group or title")
            args.name = args.workflow
            args.command = "workflow-start"
            return _cmd_workflow(args)
        if not args.backend:
            return _fail(args, "invalid_params", "--backend is required without --workflow")
        return _cmd_detached(args, _run_action(args))
    if args.command == "resume":
        return _cmd_detached(args, _resume_action(args))
    return _cmd_status(args)


if __name__ == "__main__":
    sys.exit(main())
