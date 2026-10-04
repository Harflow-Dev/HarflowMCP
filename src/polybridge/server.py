"""MCP server dispatching coding tasks to whichever agent backend the caller picks."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import shutil
import subprocess
import sys
import unicodedata
from pathlib import Path
from typing import Any

from mcp import MCPError
from pydantic import StrictBool
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.types import INTERNAL_ERROR, INVALID_PARAMS

from . import backends, control, identity, inbox, store
from .backends import DEFAULT_BACKEND, DEFAULT_FREEDOM, FREEDOMS
from .events import EVENT_KINDS, events_path, read_page, read_recent
from .tasks import (
    GIT_SAFE_CONFIG,
    GIT_SAFE_ENV,
    TERMINAL_STATUSES,
    RepoUnavailableError,
    SessionBusyError,
    SessionUnknownError,
    Task,
    TaskRegistry,
)

log = logging.getLogger("polybridge")

VALID_STATUSES = frozenset({"running"}) | TERMINAL_STATUSES

# MCP clients impose their own per-request timeout — commonly 60s — and exceeding it surfaces to the
# caller as a transport error (-32001) even though the dispatched run is unaffected. So the default
# wait stays under that, and callers are expected to come back rather than hold one request open for
# the length of a coding task.
DEFAULT_WAIT_SECONDS = 55

# Emitted while waiting so the client can see the wait is alive; per the MCP spec a client may also
# reset its request timeout on progress, which is what makes longer explicit waits viable.
PROGRESS_INTERVAL_SECONDS = 5.0

# Matches the Monitor's `TaskTitle.maxLength`.
MAX_TITLE_LENGTH = 90

# What an MCP status response carries in place of the full raw tail (see `_status_payload`).
RECENT_ACTIVITY_LIMIT = 5
SHORT_TAIL_LINES = 5
SHORT_TAIL_LINE_CHARS = 500


def _status_payload(
    snapshot: dict[str, Any], *, include_tail: bool, log_dir: Path, task_id: str
) -> dict[str, Any]:
    """The MCP shape of a status snapshot: `recent_activity` always, the raw tail only on request.

    The full `last_output_tail` is up to 20 raw stream lines of 2000 chars — about 10k tokens on
    every poll, which an orchestrator rarely needs. `recent_activity` answers "what is it doing" in
    a few one-liners instead; a failed run keeps a short raw tail, since that is when the raw lines
    help. The snapshot itself is untouched, so `polybridge-ctl` (the Monitor's contract) still
    carries the full tail. Blocking file I/O: callers run this off the event loop.
    """
    from .workflow_inspection import decorate_tasks
    payload = decorate_tasks([dict(snapshot)], log_dir)[0]
    payload["recent_activity"] = read_recent(events_path(log_dir, task_id), limit=RECENT_ACTIVITY_LIMIT)
    if include_tail:
        return payload
    tail = payload.pop("last_output_tail", None)
    if snapshot.get("status") == "failed" and isinstance(tail, list):
        payload["last_output_tail"] = [
            line[:SHORT_TAIL_LINE_CHARS] if isinstance(line, str) else line
            for line in tail[-SHORT_TAIL_LINES:]
        ]
    return payload


mcp = MCPServer(
    "polybridge",
    instructions=(
        "Dispatch coding tasks to headless coding agents on this machine — currently Claude Code, "
        "Codex, opencode, vibe and Antigravity (binary `agy`). start_task returns immediately with "
        "a task_id; poll it with get_task_status or await it with wait_for_task, then continue the "
        "same session with resume_task.\n\n"
        "Call list_backends first if you are unsure which to use: it reports what is installed and "
        "what each one can actually do. Backends differ in ways that matter — Claude and vibe "
        "support a turn cap (though a breach on vibe reads as a plain failure, not a distinct "
        "status), only Claude and opencode report a dollar cost, only Codex enforces restrictions "
        "with a real OS sandbox, only vibe has no model-selection flag at all, and Antigravity "
        "accepts reasoning_effort only at low/medium/high (its CLI refuses xhigh before the run "
        "starts). Every task reports an `enforcement` block describing what was actually "
        "enforced, which is the honest answer rather than what `freedom` implies.\n\n"
        "A `publish` freedom level sits between `write_in_repo` and `unrestricted`: it authorizes "
        "remote publishing attempts, including commits, pushes, PRs and reviews; command approvals stay in harness settings. It is NOT a promise that publishing "
        "succeeds — credentials, remote permissions, branch protection, repo hooks, or an "
        "unauthenticated `gh` can all still stop it, and the agent may not even try — and it is "
        "authorization only: with network=True a lower freedom can mechanically reach a remote "
        "without having been authorized to publish. Check "
        "`enforcement.publish_attempts_allowed_by_polybridge` and `enforcement.network_access` on "
        "the returned task rather than assuming from the freedom name alone. An optional `network` "
        "parameter on start_task and resume_task asks for network independently of the freedom: "
        "True asks polybridge to impose no network barrier of its own, False asks it to impose "
        "one, and None keeps each freedom's historical behaviour. It governs polybridge's own "
        "network barrier only, never reachability.\n\n"
        "A wait_for_task that comes back still 'running' has not failed — the run is untouched, so "
        "call again or poll. Tasks outlive this server process: ones started by an earlier "
        "polybridge server are still reported, marked 'recovered: true'.\n\n"
        "A claude task started without max_turns, and every antigravity task, has live input "
        "(`live_input: true`): send_message adds a message while it runs — folded into the "
        "running turn on claude, or starting a new one if the agent is idle; on antigravity each "
        "written message is its own turn, even mid-turn. It returns 'queued', never 'delivered'. "
        "Once every turn the run owes has been answered and nothing is queued the task closes its "
        "own input and settles as usual; a send after that is refused with 'finished; continue "
        "with resume_task'.\n\n"
        "Workflows are durable graphs owned by Polybridge. start_workflow gives the caller's "
        "original prompt and workflow context to an orchestrator harness. The orchestrator "
        "chooses valid next nodes and writes focused assignments; Polybridge validates those "
        "decisions, runs node harnesses, coordinates parallel branches, and persists results. "
        "Nodes receive their instructions, assignment, and relevant prior results, not the full "
        "request or global checklist. They return structured results and can ask the orchestrator "
        "for missing context. Only the orchestrator updates the checklist. Decisions and results "
        "come from harness final messages; harnesses may still use their own tools. Poll or wait "
        "using the workflow_run_id. For needs_input, the MCP caller answers through resume_workflow "
        "with instructions and the current input_decision_id; recover_workflow explicitly retries "
        "a settled failed run with a reason while preserving completed work."
    ),
)

_registry: TaskRegistry | None = None


def _reg() -> TaskRegistry:
    # Built lazily so its asyncio primitives belong to the loop `mcp.run()` creates.
    global _registry
    if _registry is None:
        _registry = TaskRegistry()
        _registry.start_maintenance()
    return _registry


def _backend(name: str):
    try:
        backend = backends.get(name)
    except backends.UnknownBackend as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None
    if not backends.is_installed(backend):
        # A client-launched server usually runs on the PATH it was registered with, frozen at install
        # time — so a CLI installed afterwards, or moved (an nvm Node upgrade), is missing here even
        # though a terminal finds it. Saying which PATH, and how to refresh it, makes that fixable.
        raise MCPError(
            INVALID_PARAMS,
            f"the `{backend.binary}` CLI for backend {name!r} was not found on this server's PATH "
            f"({os.pathsep.join(os.get_exec_path())}); install it, or call list_backends to see what "
            f"is available. If `{backend.binary}` is installed, polybridge's registration may be out "
            "of date: update it in Polybridge Monitor (Settings → Harnesses → Update), or run "
            f"`polybridge-setup` from a terminal where `{backend.binary}` works, then restart "
            "the client",
        )
    return backend


def _check_freedom(freedom: str) -> str:
    if freedom not in FREEDOMS:
        raise MCPError(
            INVALID_PARAMS, f"unknown freedom {freedom!r}; expected one of {list(FREEDOMS)}"
        )
    return freedom


def _check_turn_cap(backend, max_turns: int | None) -> None:
    if max_turns is None:
        return
    if max_turns < 1:
        raise MCPError(INVALID_PARAMS, f"max_turns must be >= 1, got {max_turns}")
    try:
        backends.reject_turn_cap(backend, max_turns)
    except backends.UnsupportedCapability as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


def _check_reasoning_effort(backend, reasoning_effort: str | None) -> None:
    try:
        backends.check_reasoning_effort(backend, reasoning_effort)
    except backends.UnsupportedCapability as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


def _check_model(backend, model: str | None) -> None:
    try:
        backends.reject_model(backend, model)
    except backends.UnsupportedCapability as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


def _check_group(group: str | None) -> None:
    if group is None:
        return
    if not group.strip():
        raise MCPError(INVALID_PARAMS, "group must be non-empty if provided")
    if len(group) > 128:
        raise MCPError(
            INVALID_PARAMS, f"group must be at most 128 characters, got {len(group)}"
        )


def _normalize_title(title: str | None) -> str | None:
    if title is None:
        return None
    stripped = title.strip()
    if not stripped:
        return None
    if len(stripped) > MAX_TITLE_LENGTH:
        raise MCPError(
            INVALID_PARAMS,
            f"title must be at most {MAX_TITLE_LENGTH} characters, got {len(stripped)}; "
            "shorten it or omit it",
        )
    if any(
        # Zl/Zp: U+2028/U+2029 break a line as surely as "\n" does.
        unicodedata.category(ch).startswith("C") or unicodedata.category(ch) in ("Zl", "Zp")
        for ch in stripped
    ):
        raise MCPError(
            INVALID_PARAMS,
            "title must not contain control characters (including newlines); "
            "use a single plain line of text",
        )
    return stripped


def _check_network(backend, freedom: str, network: bool | None) -> None:
    # Strict on purpose: anything other than a real boolean or None is refused rather than
    # truthy-coerced — "yes"/1/0 must not silently become a network decision, and a coerced
    # request would be indistinguishable from a considered one on the returned task.
    #
    # This check alone is NOT what delivers that on the tool surface, and believing otherwise is
    # the trap: pydantic's lax mode coerces "yes"/"true"/"on"/1/0 to real booleans while binding
    # the call, so by the time this runs the string is already gone (measured). `StrictBool` on
    # the two tool signatures is what actually refuses them; this remains as the backstop for a
    # direct Python caller, which bypasses that binding entirely.
    if network is not None and not isinstance(network, bool):
        raise MCPError(
            INVALID_PARAMS,
            f"network must be a boolean (true/false) or omitted, got {network!r}",
        )
    try:
        backends.check_network(backend, freedom, network)
    except backends.UnsupportedCapability as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


def _resolve_repo_path(repo_path: str) -> Path:
    if not repo_path or not repo_path.strip():
        raise MCPError(INVALID_PARAMS, "repo_path must be a non-empty path")

    path = Path(repo_path).expanduser()
    try:
        path = path.resolve(strict=True)
    except OSError:
        raise MCPError(INVALID_PARAMS, f"repo_path does not exist: {repo_path}") from None
    if not path.is_dir():
        raise MCPError(INVALID_PARAMS, f"repo_path is not a directory: {path}")

    probe = subprocess.run(
        ["git", "-C", str(path), *GIT_SAFE_CONFIG, "rev-parse", "--is-inside-work-tree"],
        env={**os.environ, **GIT_SAFE_ENV},
        capture_output=True,
        text=True,
        check=False,
    )
    if probe.returncode != 0 or probe.stdout.strip() != "true":
        raise MCPError(INVALID_PARAMS, f"repo_path is not inside a git repository: {path}")
    return path


async def _validate_repo_path(repo_path: str) -> Path:
    return await asyncio.to_thread(_resolve_repo_path, repo_path)


@mcp.tool()
async def list_backends() -> list[dict[str, Any]]:
    """Report the available agent backends, what is installed, and what each can actually do.

    Worth calling before start_task if the choice is not already obvious. Each entry carries its
    `capabilities` (turn cap, cost reporting, OS sandbox, per-command deny, whether we can choose
    the session id) and, for every `freedom` level, the enforcement it really delivers.
    """
    return await asyncio.to_thread(backends.describe_all)


@mcp.tool()
async def start_task(
    prompt: str,
    repo_path: str,
    backend: str | None = None,
    freedom: str | None = None,
    model: str | None = None,
    max_turns: int | None = None,
    reasoning_effort: str | None = None,
    network: StrictBool | None = None,
    group: str | None = None,
    title: str | None = None,
    workflow: str | None = None,
) -> dict[str, Any]:
    """Dispatch a coding task to a headless agent and return immediately.

    Args:
        prompt: Instructions for the agent. Be specific about the desired end state.
        repo_path: Absolute path to a git repository; the agent's working directory.
        backend: Which agent to use — "claude", "codex", "opencode", "vibe" or "antigravity". See
            list_backends.
        freedom: "read_only", "write_in_repo" (default), "publish", or "unrestricted". "publish"
            sits between "write_in_repo" and "unrestricted": it authorizes an attempt to
            remote publishing, including commit/push/PR creation/reviews, but that is not a promise the attempt succeeds — credentials,
            remote permissions, branch protection and hooks are all
            outside polybridge's control. How each level is enforced depends on the backend; the
            returned `enforcement` says what actually applies, via
            `publish_attempts_allowed_by_polybridge` and `network_access` among other fields.
        model: Model for this run, in the backend's own naming. Rejected outright, rather than
            silently ignored, on a backend with no model-selection flag at all — vibe is the one
            such case; see list_backends' capabilities.supports_model_selection.
        max_turns: Cap on agent turns. Only some backends support this; asking for it on one that
            does not is an error rather than being silently ignored.
        reasoning_effort: "low", "medium", "high" or "xhigh", passed to the backend verbatim.
            Rejected up front when the chosen *backend* has no effort control at all (vibe is
            config-only and has none), or is asked for a level outside the ones it declares (see
            list_backends — antigravity accepts only low/medium/high, and its CLI would refuse
            xhigh before the run starts, exit 1); polybridge cannot tell whether the chosen
            *model* honours it — on opencode in particular, a model with no declared variants
            silently ignores an unsupported level rather than erroring, and on antigravity a model
            id that encodes its own level makes the CLI refuse --effort at startup. See
            list_backends' per-backend reasoning_effort caveats for what is and is not known there.
        network: Optional boolean asking for network access independently of `freedom`: True
            asks polybridge to impose no network barrier of its own, False asks it to impose one,
            and None (the default) keeps each freedom's historical behaviour exactly. The
            parameter governs polybridge's own network barrier only — never reachability: a
            corporate firewall or proxy defeats it too. Only codex has a barrier polybridge can
            actually raise or lower, and its support is non-rectangular (see
            capabilities.network_control): enabling is refused at read_only and blocking at
            unrestricted. On claude, opencode, vibe and antigravity, True is accepted — there is
            nothing to impose — and False is an error rather than silently dropped;
            enforcement.network_access stays "not_controlled" there. What actually applied is
            stated on the returned task's enforcement.network_access.
        group: Optional label (1-128 chars), inherited by any nested dispatch this task's own
            agent makes through polybridge (unless that dispatch gives its own). Purely
            informational — polybridge does not act on it — useful for tagging a family of
            dispatches you want to find together later via list_tasks.
        title: Optional short human-readable label (at most 90 characters after trimming, no
            control characters) shown for this task in the Monitor. Purely informational. It is
            never inherited from a calling task: a dispatch without a title has none. A
            resume_task continuation carries the resumed task's title.

    Returns the new task_id and its starting state. The run continues in the background; poll
    get_task_status or call wait_for_task to follow it.

    If this task is itself dispatched from inside another polybridge task's agent (a nested MCP
    call), that ancestry is recorded — `spawned_by`, `root_task_id`, `depth` — on a best-effort
    basis (see `lineage_detected` on the returned task: which detection method found the caller,
    or null if none was found and this is treated as a root task). When a caller *is* detected,
    this dispatch is also checked against it: a nested dispatch weaker than its caller on depth or
    on any enforcement field is refused outright (`NestedDispatchRefused`). This cap is
    best-effort, not a sandbox boundary — nothing stops an agent from dispatching outside
    polybridge entirely.
    """
    if not prompt or not prompt.strip():
        raise MCPError(INVALID_PARAMS, "prompt must be a non-empty string")

    if workflow is not None:
        if group is not None or title is not None:
            raise MCPError(INVALID_PARAMS, "workflow runs do not accept task group or title")
        return await start_workflow(
            name=workflow, prompt=prompt, repo_path=repo_path,
            overrides={k: v for k, v in {"backend": backend, "model": model, "max_turns": max_turns,
                       "reasoning_effort": reasoning_effort}.items() if v is not None},
            freedom=freedom, network=network,
        )

    freedom = freedom if freedom is not None else DEFAULT_FREEDOM
    chosen = _backend(backend or DEFAULT_BACKEND)
    _check_freedom(freedom)
    _check_turn_cap(chosen, max_turns)
    _check_reasoning_effort(chosen, reasoning_effort)
    _check_model(chosen, model)
    _check_network(chosen, freedom, network)
    _check_group(group)
    normalized_title = _normalize_title(title)
    path = await _validate_repo_path(repo_path)

    try:
        task = await _reg().start(
            prompt,
            path,
            backend=chosen,
            freedom=freedom,
            model=model,
            max_turns=max_turns,
            reasoning_effort=reasoning_effort,
            network=network,
            group=group,
            title=normalized_title,
        )
    except backends.UnsupportedCapability as exc:
        # Covers `NestedDispatchRefused` too — it subclasses `UnsupportedCapability`, and this cap
        # is only ever checked once a caller was actually detected inside `_reg().start`.
        raise MCPError(INVALID_PARAMS, str(exc)) from None
    return task.brief() | {"enforcement": task.enforcement}


@mcp.tool()
async def get_task_status(
    task_id: str,
    include_tail: bool = False,
) -> dict[str, Any]:
    """Report a dispatched task's current state without blocking.

    Args:
        task_id: Identifier returned by start_task or resume_task.
        include_tail: If True, include the full last_output_tail (20 lines × 2000 chars each);
            if False (default), the tail is omitted for running/completed tasks and shortened
            to 5 lines × 500 chars for failed tasks. The caller can request the full raw stream
            log path from `raw_stream_log` for complete output, or use get_task_events for
            normalized events.

    Returns the task's status, summary, enforcement, and `recent_activity` — a list of ≤ 5 one-line
    strings describing the most recent meaningful events (tool calls, failed tool results, assistant
    messages, notices). For running tasks this gives a live summary without the bulk of the
    raw stream. `total_cost_usd` is null for backends that do not report cost — Codex, vibe and
    Antigravity report tokens only or nothing (vibe reports neither cost nor tokens nor a turn
    count).

    `enforcement` states what the run's restrictions actually amounted to, and `mcp_servers` (where
    the backend reports them) shows what the agent loaded. Tasks started by an earlier polybridge
    server process are still reported, marked `recovered: true` with a `note` explaining what is and
    is not known about them.
    """
    await _guard_task_read(task_id)
    task = _reg().get(task_id)
    if task is not None:
        snapshot = task.snapshot()
        return await asyncio.to_thread(
            _status_payload, snapshot, include_tail=include_tail, log_dir=_reg().log_dir, task_id=task_id
        )

    record = _reg().recover(task_id)
    if record is None:
        raise MCPError(
            INVALID_PARAMS,
            f"unknown task_id: {task_id}. No task with that id is running, and no record of one "
            f"exists under {_reg().log_dir}. Use list_tasks to see what is known.",
        )
    snapshot = store.snapshot(_reg().log_dir, record)
    return await asyncio.to_thread(
        _status_payload, snapshot, include_tail=include_tail, log_dir=_reg().log_dir, task_id=task_id
    )


async def _await_with_progress(task: Task, timeout_seconds: int, ctx: Context | None) -> None:
    """Wait for `task`, emitting progress every few seconds until it finishes or time runs out."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    steps = max(1, math.ceil(timeout_seconds / PROGRESS_INTERVAL_SECONDS))

    for step in range(1, steps + 1):
        remaining = deadline - loop.time()
        if remaining <= 0:
            return
        try:
            await asyncio.wait_for(
                task.done.wait(), timeout=min(PROGRESS_INTERVAL_SECONDS, remaining)
            )
            return
        except asyncio.TimeoutError:
            pass

        if ctx is None:
            continue
        try:
            await ctx.report_progress(
                step,
                total=steps,
                message=(
                    f"{task.backend} task {task.task_id} still running "
                    f"({task.duration_seconds:.0f}s elapsed)"
                ),
            )
        except Exception:  # pragma: no cover - a client that ignores progress must not break us
            log.debug("task %s: client did not accept progress", task.task_id)


async def _poll_recovered(
    record: store.TaskRecord, timeout_seconds: int, ctx: Context | None
) -> store.TaskRecord:
    """Wait on a task from another server process by polling its liveness.

    There is no completion event to await here, so this checks the recorded process on the same
    cadence it reports progress, and re-reads the record in case that process's own server finishes
    it first.

    Whether it has settled is decided by `store.resolve_status`, never by the record's own `status`
    field: a record can claim `failed` about a process that is still running (see
    `store.outcome_unobserved`), and believing it would end the wait after a single interval while
    the caller is told the whole timeout elapsed.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    steps = max(1, math.ceil(timeout_seconds / PROGRESS_INTERVAL_SECONDS))

    for step in range(1, steps + 1):
        if loop.time() >= deadline:
            break
        if store.resolve_status(_reg().log_dir, record, detail=False)[0] in TERMINAL_STATUSES:
            break
        await asyncio.sleep(min(PROGRESS_INTERVAL_SECONDS, max(0.0, deadline - loop.time())))

        fresh = _reg().recover(record.task_id)
        if fresh is not None:
            record = fresh

        if ctx is None:
            continue
        try:
            await ctx.report_progress(
                step, total=steps, message=f"{record.task_id} still running (recovered task)"
            )
        except Exception:  # pragma: no cover
            log.debug("task %s: client did not accept progress", record.task_id)

    return record


@mcp.tool()
async def wait_for_task(
    task_id: str,
    timeout_seconds: int = DEFAULT_WAIT_SECONDS,
    include_tail: bool = False,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Wait for a task to finish, giving up after a timeout without disturbing the run.

    Args:
        task_id: Identifier of the task to await.
        timeout_seconds: How long to wait before returning early. Keep this modest — your MCP client
            applies its own request timeout (often 60s), and exceeding it fails the *call* with a
            timeout error even though the task keeps running.
        include_tail: If True, include the full last_output_tail (20 lines × 2000 chars each);
            if False (default), the tail is omitted for running/completed tasks and shortened
            to 5 lines × 500 chars for failed tasks.
        ctx: Injected by the server; not a caller argument.

    If the task is still going when the wait ends, the result comes back with status "running" and a
    `next_step` hint: call this again, or poll get_task_status. The dispatched process is never
    signalled here, so waiting is always safe and repeating it costs nothing. Returns
    `recent_activity` — a list of ≤ 5 one-line strings describing the most recent meaningful events
    — for a live summary without the bulk of the raw stream.

    A coding task can easily run for many minutes. Expect several calls rather than one long one.
    """
    await _guard_task_read(task_id)
    if timeout_seconds <= 0:
        raise MCPError(INVALID_PARAMS, f"timeout_seconds must be > 0, got {timeout_seconds}")

    task = _reg().get(task_id)
    if task is not None:
        await _await_with_progress(task, timeout_seconds, ctx)
        snapshot = task.snapshot()
        finished = task.finished
    else:
        record = _reg().recover(task_id)
        if record is None:
            raise MCPError(
                INVALID_PARAMS,
                f"unknown task_id: {task_id}. Use list_tasks to see what is known.",
            )
        record = await _poll_recovered(record, timeout_seconds, ctx)
        snapshot = store.snapshot(_reg().log_dir, record)
        finished = snapshot["status"] in TERMINAL_STATUSES

    if not finished:
        log.info("task %s still running after %ss wait", task_id, timeout_seconds)
        snapshot["next_step"] = (
            f"still running after {timeout_seconds}s and unaffected by this wait — "
            "call wait_for_task again, or poll get_task_status"
        )

    return await asyncio.to_thread(
        _status_payload, snapshot, include_tail=include_tail, log_dir=_reg().log_dir, task_id=task_id
    )


@mcp.tool()
async def resume_task(
    task_id: str,
    followup_prompt: str,
    max_turns: int | None = None,
    network: StrictBool | None = None,
) -> dict[str, Any]:
    """Continue a finished task's session with follow-up instructions.

    Args:
        task_id: A task that has finished; its session is the one resumed.
        followup_prompt: What the agent should do next, with the prior context intact.
        max_turns: Cap on agent turns, where the backend supports one.
        network: Optional boolean overriding the parent run's network setting for this run only:
            None (the default) inherits what the parent was dispatched with — which is also the
            historical default for a record written before the field existed — True asks for no
            network barrier of polybridge's own, False asks for one, honoured or refused exactly
            as on start_task for the parent's freedom.

    Returns a new task_id sharing the original session, and returns immediately as with start_task.
    Runs on the same backend, model, reasoning_effort and freedom as the original — none of these
    can be changed on resume, since a mid-conversation switch would not honestly describe what
    produced the reply. `network` is the one per-run override, because it is a per-run sandbox
    setting rather than a description of the session.

    Some backends mint their own session id and only disclose it mid-run; if a task died before
    doing so, its conversation cannot be continued and this says so rather than starting a
    disconnected one.
    """
    if not followup_prompt or not followup_prompt.strip():
        raise MCPError(INVALID_PARAMS, "followup_prompt must be a non-empty string")
    # Same strict boolean rule as start_task, checked before anything is resumed.
    if network is not None and not isinstance(network, bool):
        raise MCPError(
            INVALID_PARAMS,
            f"network must be a boolean (true/false) or omitted, got {network!r}",
        )

    parent = _reg().get(task_id)
    try:
        if parent is not None:
            # `finished`, not the status field: a cancellation in progress is not resumable yet.
            if not parent.finished:
                raise MCPError(
                    INVALID_PARAMS,
                    f"task {task_id} is still {parent.status}; wait for it or cancel it "
                    "before resuming",
                )
            _check_turn_cap(backends.get(parent.backend), max_turns)
            _check_network(backends.get(parent.backend), parent.freedom, network)
            task = await _reg().resume(
                parent, followup_prompt, max_turns=max_turns, network=network
            )
        else:
            record = _reg().recover(task_id)
            if record is None:
                raise MCPError(INVALID_PARAMS, f"unknown task_id: {task_id}")
            if store.record_process_alive(record):
                raise MCPError(
                    INVALID_PARAMS,
                    f"task {task_id} is still running (started by an earlier polybridge server "
                    "process); wait for it or cancel it before resuming",
                )
            _check_turn_cap(backends.get(record.backend), max_turns)
            _check_network(backends.get(record.backend), record.freedom, network)
            task = await _reg().resume_record(
                record, followup_prompt, max_turns=max_turns, network=network
            )
    except (SessionBusyError, SessionUnknownError, RepoUnavailableError) as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None
    except (backends.UnknownBackend, backends.UnsupportedCapability) as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None
    return task.brief() | {"enforcement": task.enforcement}


@mcp.tool()
async def list_tasks(status: str | None = None, backend: str | None = None) -> list[dict[str, Any]]:
    """List dispatched tasks, oldest first.

    Args:
        status: Optional filter — running, completed, failed, timed_out or cancelled.
        backend: Optional filter by agent, e.g. "codex".

    Includes tasks from earlier polybridge server processes, marked `recovered: true`.
    """
    if status is not None and status not in VALID_STATUSES:
        raise MCPError(
            INVALID_PARAMS, f"unknown status {status!r}; expected one of {sorted(VALID_STATUSES)}"
        )
    if backend is not None and backend not in backends.BACKENDS:
        raise MCPError(
            INVALID_PARAMS,
            f"unknown backend {backend!r}; expected one of {sorted(backends.BACKENDS)}",
        )

    live = _reg().list()
    entries = [task.brief() for task in live]
    entries.extend(_reg().recovered_briefs(exclude={task.task_id for task in live}))
    entries = await _filter_task_reads(entries)
    entries.sort(key=lambda entry: entry["started_at"])

    if status is not None:
        entries = [entry for entry in entries if entry["status"] == status]
    if backend is not None:
        entries = [entry for entry in entries if entry.get("backend") == backend]
    from .workflow_inspection import decorate_tasks
    return await asyncio.to_thread(decorate_tasks, entries, _reg().log_dir)


@mcp.tool()
async def cancel_task(task_id: str) -> dict[str, Any]:
    """Stop a running task, terminating the agent and any processes it spawned, and cascade the
    same stop to any live descendants this bridge can find.

    Args:
        task_id: Identifier of the task to stop.

    Already-finished tasks are returned unchanged. Work the agent had already written to disk is
    left in place. Tasks started by an earlier polybridge server process can be stopped too, via
    their recorded process group.

    The cascade reaches nested dispatches that task's own agent made through polybridge —
    `spawned_by`/`root_task_id` lineage, best-effort, not a sandboxed guarantee — and the response
    carries the result as `cascade`: `cancelled_descendants` (ids now cancelled),
    `sigkill_survivors` (ids that resisted even SIGKILL), `owner_still_settling` (ids belonging to
    a still-alive bridge server that has not settled them yet), `not_signalled` (ids that
    could not be signalled at all, with why), and `not_recorded` (ids that were signalled but
    whose `cancelled` status could not be written, with why — their records are left untouched).
    """
    if _reg().get(task_id) is None and _reg().recover(task_id) is None:
        raise MCPError(INVALID_PARAMS, f"unknown task_id: {task_id}")

    try:
        cascade = await _reg().cancel_cascade(task_id)
    except backends.UnsupportedCapability as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None
    except control.PhaseWriteError as exc:
        raise MCPError(
            INTERNAL_ERROR,
            f"the cancel for task {task_id} was not attempted because its phase file could not "
            f"be written, so nothing was signalled: {exc}",
        ) from None

    # Snapshot the state the cascade produced, not what we started from, or the response would
    # report a status that later calls contradict.
    task = _reg().get(task_id)
    if task is not None:
        snapshot = task.snapshot()
    else:
        record = _reg().recover(task_id)
        snapshot = store.snapshot(_reg().log_dir, record) if record is not None else {}
    
    response = await asyncio.to_thread(
        _status_payload, snapshot, include_tail=False, log_dir=_reg().log_dir, task_id=task_id
    )
    response["cascade"] = cascade
    return response


@mcp.tool()
async def get_task_events(
    task_id: str,
    limit: int = 50,
    before_seq: int | None = None,
    after_seq: int | None = None,
    kinds: list[str] | None = None,
) -> dict[str, Any]:
    """Fetch normalized events from a task's event log.

    Args:
        task_id: Identifier of the task whose events to read.
        limit: Maximum number of events to return, capped at 200. Default is 50.
        before_seq: Exclusive upper bound — return events with seq < before_seq (older events).
            Cannot be used with after_seq.
        after_seq: Exclusive lower bound — return events with seq > after_seq (newer events).
            Cannot be used with before_seq.
        kinds: Optional filter by event kind. Must be a non-empty list of known kinds from
            `task_started`, `assistant_text`, `assistant_delta`, `tool_call`, `tool_result`,
            `user_message`, `usage`, `notice`, `task_finished`, `undelivered`.
            `assistant_delta` is excluded by default unless explicitly listed. Pass `null`
            to get all kinds except assistant_delta.

    Returns events in oldest→newest order within the page, with:
    - `events`: list of event objects with their `seq`, `kind`, and data fields
    - `has_more`: true if there are more matching events in the requested direction
    - `next_before_seq`: the `seq` of the oldest event in this page (for paging older)
    - `next_after_seq`: the `seq` of the newest event in this page (for paging newer)
    - `skipped_oversized`: count of lines > 1 MiB that were skipped (not parsed, not counted as events)

    Each returned event has its long string fields truncated to 2000 chars, lists to 50 items,
    and dict nesting to depth 4, with `"truncated": true` added if anything was cut. An event still
    over 16 KiB after that (a very wide dict) is replaced by a stub carrying only `seq`, `kind`,
    `observed_at`, `truncated: true`, `oversized: true` and its size in `bytes`. A page
    stops early once its serialized size would pass 256 KiB (has_more remains true, cursors still
    valid). A page is never empty while has_more is true.

    Default (no before_seq/after_seq): newest page — the most recent events.
    Both cursors cannot be used at once. kinds=[] or an unknown kind raises INVALID_PARAMS.
    """
    await _guard_task_read(task_id)
    try:
        store.validate_task_id(task_id)
    except store.InvalidTaskId as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None

    if limit < 1 or limit > 200:
        raise MCPError(INVALID_PARAMS, f"limit must be between 1 and 200, got {limit}")

    if before_seq is not None and after_seq is not None:
        raise MCPError(
            INVALID_PARAMS, "cannot specify both before_seq and after_seq; use one or the other"
        )

    if kinds is not None:
        if not kinds:
            raise MCPError(INVALID_PARAMS, "kinds must be a non-empty list")
        for kind in kinds:
            if kind not in EVENT_KINDS:
                raise MCPError(
                    INVALID_PARAMS,
                    f"unknown kind {kind!r}; must be one of {sorted(EVENT_KINDS)}",
                )

    task = _reg().get(task_id)
    record = _reg().recover(task_id) if task is None else None

    if task is None and record is None:
        raise MCPError(INVALID_PARAMS, f"unknown task_id: {task_id}")

    page_result = await asyncio.to_thread(
        read_page,
        events_path(_reg().log_dir, task_id),
        limit=limit,
        before_seq=before_seq,
        after_seq=after_seq,
        kinds=kinds,
    )

    return {
        "task_id": task_id,
        "events": page_result.events,
        "has_more": page_result.has_more,
        "next_before_seq": page_result.next_before_seq,
        "next_after_seq": page_result.next_after_seq,
        "skipped_oversized": page_result.skipped_oversized,
    }


@mcp.tool()
async def send_message(task_id: str, text: str) -> dict[str, Any]:
    """Add a message to a running live-input task, as if the user had typed it mid-run.

    Args:
        task_id: A running task whose `live_input` is true (claude started without `max_turns`,
            or any antigravity task).
        text: The message.

    Returns `status: "queued"` — never "delivered". The task's input pump writes the message to the
    agent: on claude, a message arriving while a turn is running is folded into that turn, and one
    arriving while the agent is idle starts a new turn; on antigravity every written message is its
    own turn, even mid-turn. The task's event log then records a `user_message` event
    (`source: "injected"`) when it is written, or an `undelivered` event plus a notice if it never
    is (the run errored or exited first).

    A live task closes its own input once it is idle with nothing queued, so it still settles
    unattended. A send after that is refused with "finished; continue with resume_task" — which is
    exactly what to do. Also refused: a task that was not started with live input, one that has
    settled, and one whose owning polybridge server is not confirmed alive.
    """
    try:
        store.validate_task_id(task_id)
    except store.InvalidTaskId as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None
    if not isinstance(text, str) or not text.strip():
        raise MCPError(INVALID_PARAMS, "text must be a non-empty string")

    try:
        caller = await _verified_workflow_caller()
        if caller is not None:
            from .workflow_hooks import refuse_message_caller
            refuse_message_caller(_reg().log_dir, caller.record.task_id)
        task = _reg().get(task_id)
        if task is not None:
            return await _reg().send_message(task, text)
        if _reg().recover(task_id) is None:
            raise MCPError(INVALID_PARAMS, f"unknown task_id: {task_id}")
        return await _reg().send_to_record(task_id, text)
    except inbox.SendRefused as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


async def _filter_task_reads(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from .workflow_inspection import filter_task_reads
    return filter_task_reads(entries, await _managed_workflow_reader())


async def _verified_workflow_caller():
    """Read/mutation authority must distinguish a human from failed detection."""
    from . import lineage
    caller = await _reg()._detect_caller()
    if caller is not None:
        return caller
    detection = await asyncio.to_thread(lineage.detect_caller_detail, _reg().log_dir)
    if detection.undecidable is not None:
        raise MCPError(INVALID_PARAMS, "Workflow caller authority is undecidable: " + str(detection.undecidable))
    if detection.caller is not None:
        return detection.caller
    if os.environ.get(lineage.ENV_TASK_ID):
        raise MCPError(INVALID_PARAMS, "Workflow caller task identity cannot be verified")
    return None


async def _managed_workflow_reader() -> tuple[dict[str, Any], dict[str, Any]] | None:
    from . import workflow_hooks, workflows
    caller = await _verified_workflow_caller()
    if caller is None:
        return None
    association = workflow_hooks.owner(_reg().log_dir, caller.record.task_id, strict=True)
    if association is None or association.get("role") == "builder":
        return None
    run = workflows.WorkflowStore(root=_reg().log_dir.parent).get_run(association["workflow_run_id"])
    if run.get("execution_contract") != "delegation":
        return None
    return association, run


def _managed_run_summary(run: dict[str, Any]) -> dict[str, Any]:
    """Expose routing state without bypassing settled-only result inspection."""
    keys = ("workflow_run_id", "name", "kind", "status", "definition", "revision", "prompt", "execution_contract", "tasks", "checklist_disposition", "decisions", "transitions", "settling", "input_question", "input_decision_id", "interaction_owner", "reason", "instructions", "created_at", "updated_at")
    summary = {key: run[key] for key in keys if key in run}
    if isinstance(run.get("technical_plan"), str):
        summary.update(technical_plan=run["technical_plan"][:16000], technical_plan_truncated=len(run["technical_plan"]) > 16000, technical_plan_execution_id=run.get("technical_plan_execution_id"))
    summary["activations"] = [{key: activation[key] for key in ("id", "node_id", "role", "status", "created_at", "finished_at") if key in activation} | {"tasks": [{"task_id": task["task_id"], "status": task["status"]} for task in activation.get("tasks", [])]} for activation in run.get("activations", [])]
    return summary


async def _guard_task_read(task_id: str) -> None:
    from .workflow_inspection import guard_task_read
    try:
        guard_task_read(task_id, await _managed_workflow_reader())
    except ValueError as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


def _guard_saved_workflow_authority(caller: Any, definition: dict[str, Any]) -> None:
    """An ordinary agent may save only capabilities inside its recorded envelope."""
    from . import workflows
    from .backends.base import check_nested_enforcement
    record = caller.record
    if record.freedom not in FREEDOMS:
        raise ValueError("Saving a workflow requires known caller freedom")
    configs = [(definition["orchestrator"], "read_only", None)]
    configs.extend((node["agent"], workflows.effective_freedom(node, "unrestricted", permission_policy="saved_node"), node.get("network")) for node in definition["nodes"] if node["type"] == "agent")
    for config, freedom, network in configs:
        if FREEDOMS.index(freedom) > FREEDOMS.index(record.freedom):
            raise ValueError("Saved workflow access cannot exceed the caller's freedom")
        if network is True and record.network is False:
            raise ValueError("Saved workflow network cannot exceed the caller's network restriction")
        for candidate in [config, *config.get("fallbacks", [])]:
            backend_ = backends.get(candidate["backend"])
            enforcement = backend_.enforcement(freedom, network)
            check_nested_enforcement(record.enforcement or {}, enforcement, parent_backend=record.backend, child_backend=backend_.name, parent_repo=record.repo_path, child_repo=record.repo_path)


async def _workflow_call(action: str, **kwargs: Any) -> Any:
    from . import workflows
    try:
        if kwargs.get("interaction_owner") == "monitor":
            from .takeover import caller_refusal
            refusal = await asyncio.to_thread(caller_refusal, _reg().log_dir)
            if refusal is not None:
                raise ValueError("Only a verified human Monitor caller can claim monitor interaction ownership: " + refusal[1])
        if action in {"pause", "resume", "recover"}:
            kwargs.setdefault("interaction_owner", "caller")
        if action not in {"list", "list_runs", "get", "status", "inspect", "detail"}:
            from .workflow_hooks import refuse_managed
            caller = await _verified_workflow_caller()
            if caller is not None:
                refuse_managed(_reg().log_dir, caller.record.task_id)
        if action == "save" and caller is not None:
            definition = workflows.validate_definition({**kwargs["definition"], "name": kwargs["name"]})
            _guard_saved_workflow_authority(caller, definition)
        if action == "start":
            if kwargs.get("freedom") is not None:
                raise ValueError("Workflow access is defined by saved nodes; caller freedom overrides are not supported")
            kwargs["definition_snapshot"] = workflows.WorkflowStore().get(kwargs["name"])
        if action in {"start", "build"}:
            if not kwargs["prompt"] or not kwargs["prompt"].strip():
                raise ValueError("prompt must be a non-empty string")
            if action == "build":
                workflows._candidate({**kwargs["agent"], "fallbacks": kwargs.get("fallbacks") or kwargs["agent"].get("fallbacks", [])})
            caller = await _verified_workflow_caller()
            if caller is not None:
                configs = []
                if action == "start":
                    definition = kwargs["definition_snapshot"]
                    orchestrator = {**definition["orchestrator"], **(kwargs.get("overrides") or {})}
                    configs.append((orchestrator, "read_only", kwargs.get("network")))
                    configs.extend((n["agent"], workflows.effective_freedom(n, "unrestricted", permission_policy="saved_node"), False if kwargs.get("network") is False else n.get("network", kwargs.get("network"))) for n in definition["nodes"] if n["type"] == "agent")
                else:
                    configs.append(({**kwargs["agent"], "fallbacks": kwargs.get("fallbacks") or []}, "read_only", None))
                path = await _validate_repo_path(kwargs["repo_path"]) if kwargs.get("repo_path") is not None else workflows.builder_workspace(workflows.WorkflowStore())
                for config, freedom, network in configs:
                    for candidate in [config, *config.get("fallbacks", [])]:
                        backend_ = backends.get(candidate["backend"])
                        try:
                            enforcement = backend_.enforcement(freedom, network)
                        except backends.NestedDispatchRefused:
                            raise
                        except backends.UnsupportedCapability:
                            # The supervisor journals this candidate refusal and tries its fallback.
                            continue
                        _reg()._resolve_lineage(caller, child_enforcement=enforcement, child_backend=backend_.name, child_repo=path)
        if action == "start":
            kwargs["repo_path"] = await _validate_repo_path(kwargs["repo_path"])
            return await workflows.start_workflow(**kwargs)
        if action == "build":
            if kwargs.get("repo_path") is not None:
                kwargs["repo_path"] = await _validate_repo_path(kwargs["repo_path"])
            return await workflows.build_workflow(**kwargs)
        if action == "builder_followup":
            return await workflows.followup_workflow_builder(**kwargs)
        managed = await _managed_workflow_reader()
        if managed is not None:
            association, owned_run = managed
            if association["role"] != "orchestrator":
                raise ValueError("Worker nodes cannot inspect workflow context")
            if action in {"status", "inspect", "detail"} and kwargs["run_id"] != owned_run["workflow_run_id"]:
                raise ValueError("Orchestrators may only inspect their own workflow run")
            if action == "get":
                if kwargs["name"] != owned_run["name"]:
                    raise ValueError("Orchestrators may only inspect their current workflow graph")
                return owned_run["definition"]
            if action == "list":
                return [owned_run["definition"]]
            if action == "list_runs":
                return [_managed_run_summary(owned_run)]
            if action == "detail":
                from .workflow_responses import PUBLIC_VIEWS
                if kwargs["view"] == "executions" or kwargs["view"] not in PUBLIC_VIEWS:
                    raise ValueError("Use inspect_workflow_node for settled execution details")
                from .workflow_responses import detail
                return detail(owned_run, kwargs["view"], kwargs.get("cursor"), kwargs.get("limit", 8000))
            if action == "status":
                return _managed_run_summary(owned_run)
        store_ = workflows.WorkflowStore()
        if action == "list":
            return await asyncio.to_thread(store_.list)
        if action == "list_runs":
            return await asyncio.to_thread(store_.list_runs)
        if action == "get":
            return await asyncio.to_thread(store_.get, kwargs["name"])
        if action == "save":
            return await asyncio.to_thread(store_.save, **kwargs)
        if action == "delete":
            return await asyncio.to_thread(store_.delete, kwargs["name"])
        if action == "inspect":
            from .workflow_inspection import inspect_request
            run = await asyncio.to_thread(store_.get_run, kwargs["run_id"])
            request = {key: value for key, value in kwargs.items() if key != "run_id"}
            return await asyncio.to_thread(inspect_request, run, store_.root, request)
        if action == "detail":
            from .workflow_responses import detail
            run = await asyncio.to_thread(store_.get_run, kwargs["run_id"])
            return detail(run, kwargs["view"], kwargs.get("cursor"), kwargs.get("limit", 8000))
        if action == "status":
            return await asyncio.to_thread(store_.get_run, kwargs["run_id"])
        return await asyncio.to_thread(store_.control, action=action, **kwargs)
    except (ValueError, KeyError, OSError, backends.UnsupportedCapability) as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


@mcp.tool()
async def list_workflows() -> list[dict[str, Any]]:
    """List saved workflow definitions."""
    return await _workflow_call("list")


@mcp.tool()
async def get_workflow(name: str) -> dict[str, Any]:
    """Read a saved workflow definition and revision."""
    return await _workflow_call("get", name=name)


@mcp.tool()
async def save_workflow(name: str, definition: dict[str, Any], expected_revision: int | None = None) -> dict[str, Any]:
    """Validate and save an explicit workflow; expected_revision protects concurrent edits.

    Set routing_mode="explicit". Ordinary nodes choose exactly one outgoing path.
    Parallel execution uses paired parallel_start/parallel_end nodes with shared
    parallel_group_id. Every branch reaches its matching end; nesting is supported.
    Agent callers may save only access and network settings within their recorded
    capability envelope; verified human callers can author workflow permissions.
    """
    return await _workflow_call("save", name=name, definition=definition, expected_revision=expected_revision)


@mcp.tool()
async def delete_workflow(name: str) -> dict[str, Any]:
    """Delete a definition; historical run snapshots remain available."""
    return await _workflow_call("delete", name=name)


@mcp.tool()
async def workflow_builder(name: str, prompt: str, repo_path: str | None = None, agent: dict[str, Any] | None = None, fallbacks: list[dict[str, Any]] | None = None, definition: dict[str, Any] | None = None, source: dict[str, Any] | None = None) -> dict[str, Any]:
    """Generate a workflow, or refine the current canvas into an unsaved validated proposal.

    Layout uses top-left logical point coordinates with x/y >= 0 and a 10-point dot grid.
    Agent nodes are 200 x 92 points (reserve 20 x 10 grid cells); Start/End are 72 x 72
    (reserve 8 x 8 cells). Leave at least 40 points (4 cells) between node edges and avoid
    overlaps. Arrange forward steps left to right and parallel branches on separate rows.
    Use routing_mode="explicit". Ordinary outgoing paths are exclusive alternatives.
    Parallel execution requires paired parallel_start/parallel_end structural nodes with
    a shared parallel_group_id; every branch must reach its matching end. Parallel start
    runs all branches, which may contain multiple steps and properly nested groups.
    Preserve existing node positions exactly unless the user explicitly asks to move or rearrange them.
    The canvas expands automatically; there is no fixed right or bottom boundary.
    """
    if agent is None:
        raise MCPError(INVALID_PARAMS, "Builder agent is required")
    return await _workflow_call("build", name=name, prompt=prompt, repo_path=repo_path, agent=agent, fallbacks=fallbacks, definition=definition, source=source)


@mcp.tool()
async def followup_workflow_builder(run_id: str, prompt: str) -> dict[str, Any]:
    """Queue a message in the same workflow-builder conversation without saving its draft."""
    return await _workflow_call("builder_followup", run_id=run_id, prompt=prompt)


@mcp.tool()
async def apply_workflow_draft(definition: dict[str, Any], expected_draft_revision: int) -> dict[str, Any]:
    """Publish a preview from the verified active builder task; never write a saved workflow.

    Positions are top-left logical points, with finite x/y >= 0, on a 10-point dot grid.
    Agent nodes are 200 x 92 points (reserve 20 x 10 grid cells); Start/End are 72 x 72
    (reserve 8 x 8 cells). Leave at least 40 points (4 cells) between node edges; no overlaps.
    Use routing_mode="explicit". Ordinary outgoing paths are exclusive alternatives.
    Parallel execution requires paired parallel_start/parallel_end structural nodes with
    a shared parallel_group_id; every branch must reach its matching end. Parallel start
    runs all branches, which may contain multiple steps and properly nested groups.
    Preserve existing node positions exactly unless the user explicitly asks to move or rearrange them.
    The canvas expands automatically; do not constrain nodes to a fixed viewport.
    """
    from . import workflows
    try:
        caller = await _reg()._detect_caller()
        return await workflows.apply_workflow_draft(definition, expected_draft_revision, caller=caller)
    except (ValueError, KeyError, OSError) as exc:
        raise MCPError(INVALID_PARAMS, str(exc)) from None


@mcp.tool()
async def start_workflow(name: str, prompt: str, repo_path: str, overrides: dict[str, Any] | None = None, freedom: str | None = None, network: StrictBool | None = None) -> dict[str, Any]:
    """Start a durable workflow run; returns workflow_run_id rather than task_id."""
    from .workflow_responses import compact
    return compact(await _workflow_call("start", name=name, prompt=prompt, repo_path=repo_path, overrides=overrides, freedom=freedom, network=network))


@mcp.tool()
async def list_workflow_runs(offset: int = 0, limit: int = 10) -> dict[str, Any]:
    """List recorded workflow runs."""
    from .workflow_responses import compact
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 20:
        raise MCPError(INVALID_PARAMS, "offset must be nonnegative; limit must be 1–20")
    runs = await _workflow_call("list_runs")
    entries = []
    for run in runs[offset:offset + limit]:
        candidate = compact(run)
        import json
        if len(json.dumps({"runs": entries + [candidate]}, ensure_ascii=True).encode()) > 22 * 1024:
            break
        entries.append(candidate)
    next_offset = offset + len(entries)
    return {"response_version": 1, "runs": entries, "next_offset": next_offset if next_offset < len(runs) else None}


@mcp.tool()
async def get_workflow_status(workflow_run_id: str) -> dict[str, Any]:
    """Read a workflow run's execution state and task associations."""
    from .workflow_responses import compact
    return compact(await _workflow_call("status", run_id=workflow_run_id))


async def _wait_workflow_full(workflow_run_id: str, timeout_seconds: int = 30) -> dict[str, Any]:
    if type(timeout_seconds) is not int or not 0 <= timeout_seconds <= 300:
        raise MCPError(INVALID_PARAMS, "timeout_seconds must be between 0 and 300")
    effective = min(timeout_seconds, 45)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + effective
    result = await _workflow_call("status", run_id=workflow_run_id)
    path = Path.home() / ".polybridge" / "workflow-runs" / (workflow_run_id + ".json")
    def revision():
        try:
            stat = path.stat()
            return stat.st_mtime_ns, stat.st_size
        except OSError:
            return None
    # First poll refreshes after the initial read, avoiding a read/stat race.
    previous_revision = None
    while True:
        active = result.get("status") in {"running", "pending", "building", "pausing", "starting", "cancelling"}
        if not active or loop.time() >= deadline:
            return {**result, "requested_timeout_seconds": timeout_seconds, "effective_timeout_seconds": effective, "timed_out": active, "polling_guidance": "Poll wait_for_workflow again while running; server holds at most 45 seconds."}
        await asyncio.sleep(min(1, max(0, deadline - loop.time())))
        current_revision = revision()
        if current_revision is None or current_revision != previous_revision:
            result = await _workflow_call("status", run_id=workflow_run_id)
            previous_revision = current_revision


@mcp.tool()
async def wait_for_workflow(workflow_run_id: str, timeout_seconds: int = 30) -> dict[str, Any]:
    """Wait for attention/completion, default 30s; effective hold capped at 45s.

    Requests up to 300s are accepted but capped; inspect returned timeout and polling fields.
    """
    from .workflow_responses import compact
    run = await _wait_workflow_full(workflow_run_id, timeout_seconds)
    return compact(run) | {key: run[key] for key in ("requested_timeout_seconds", "effective_timeout_seconds", "timed_out", "polling_guidance")}


@mcp.tool()
async def get_workflow_run_detail(workflow_run_id: str, view: str, cursor: str | None = None, limit: int = 8000) -> dict[str, Any]:
    """Losslessly read executions, decisions, checklist, technical_plan, definition, builder_draft, generated_definition, question, reason or wait_reason.

    Concatenate chunk fields then decode JSON. Cursors bind content, run and view; changed
    content requires restarting. Managed orchestrators use settled node inspection for executions.
    """
    return await _workflow_call("detail", run_id=workflow_run_id, view=view, cursor=cursor, limit=limit)


@mcp.tool()
async def pause_workflow(workflow_run_id: str) -> dict[str, Any]:
    """Stop scheduling while active steps settle."""
    from .workflow_responses import compact
    return compact(await _workflow_call("pause", run_id=workflow_run_id))


@mcp.tool()
async def resume_workflow(workflow_run_id: str, instructions: str | None = None, additional_attempts: int = 0, decision_id: str | None = None) -> dict[str, Any]:
    """Resume a caller-owned run with instructions and optional explicit attempt grants.

    For needs_input, supply its current input_decision_id as decision_id and a nonempty answer
    in instructions. Stale answers and live/uncertain dispatches are refused.
    """
    from .workflow_responses import compact
    return compact(await _workflow_call("resume", run_id=workflow_run_id, instructions=instructions, additional_attempts=additional_attempts, decision_id=decision_id))


@mcp.tool()
async def inspect_workflow_node(workflow_run_id: str, execution_id: str, task_id: str | None = None, view: str = "result", cursor: str | None = None, limit: int | None = None, before_seq: int | None = None, after_seq: int | None = None) -> dict[str, Any]:
    """Inspect any fully settled node execution in your workflow, including older executions.

    Result view returns lossless JSON text chunks: concatenate chunk fields until next_cursor
    is null, then decode JSON. Pass next_cursor back as cursor. Default payload contains the
    normalized node_result and full raw_output. Selecting task_id reads a specific fallback
    attempt. Activity uses normalized events with before_seq/after_seq. Result limit is
    characters (default 16000, max 32000); activity limit is events (default 50, max 200).
    No live or uncertain execution can be inspected.
    """
    if view == "result" and (before_seq is not None or after_seq is not None):
        raise MCPError(INVALID_PARAMS, "Result view uses cursor, not event sequence cursors")
    return await _workflow_call("inspect", run_id=workflow_run_id, execution_id=execution_id, task_id=task_id, view=view, cursor=cursor, limit=limit if limit is not None else (50 if view == "activity" else 16000), before_seq=before_seq, after_seq=after_seq)


@mcp.tool()
async def recover_workflow(workflow_run_id: str, reason: str, additional_attempts: int = 0) -> dict[str, Any]:
    """Explicitly recover a failed settled run; retain completed work and supply a reason."""
    if not reason or not reason.strip():
        raise MCPError(INVALID_PARAMS, "Recovery reason must be nonempty")
    from .workflow_responses import compact
    return compact(await _workflow_call("recover", run_id=workflow_run_id, instructions=reason, additional_attempts=additional_attempts))


@mcp.tool()
async def cancel_workflow(workflow_run_id: str) -> dict[str, Any]:
    """Stop scheduling and cancel the workflow's associated tasks."""
    from .workflow_responses import compact
    return compact(await _workflow_call("cancel", run_id=workflow_run_id))


def main() -> None:
    """Entry point for the `polybridge-server` console script."""
    logging.basicConfig(
        level=os.environ.get("PB_LOG_LEVEL", "INFO").upper(),
        # stderr, never stdout: stdout is the MCP wire.
        stream=sys.stderr,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    available = [
        name for name, backend in backends.BACKENDS.items() if shutil.which(backend.binary)
    ]
    missing = sorted(set(backends.BACKENDS) - set(available))
    log.info("polybridge starting (backends available: %s)", ", ".join(available) or "none")
    if missing:
        log.warning("backends unavailable, their CLI is not on PATH: %s", ", ".join(missing))
    # Warmed here rather than left to the first dispatch, so every task's `owner` is computed
    # once at startup instead of paying a `ps` call on the first `start_task`.
    identity.own_identity()
    mcp.run()


if __name__ == "__main__":
    main()
