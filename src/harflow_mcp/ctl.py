"""harflow-ctl: a CLI over the task records `harflow-server` writes to disk.

Deliberately separate from the MCP server, and it never starts retention. `list`, `status` and
`status-batch` only read what `store` and `tasks.default_log_dir` already expose (which is also
why `list`'s status resolution is cheap for a settled task — see `store.resolve_status`'s
`detail=False` shortcut), and never construct a `TaskRegistry`. The workflow node reads
(`inspect`/`assigned-input`) share the server's checks through `workflow_node_reads` and never
import the server module; the ctl paths that do need registry-backed server helpers install one
with `server.install_cli_registry`, which starts neither the maintenance sweep nor the
notification worker. `send` appends a message to a live-input task's inbox under that
inbox's lock (see `inbox.py`) for the owning server to deliver, or queues it in the follow-up
ledger (`followups.py`) of a task without live input, for its next continuation.

The control commands act (A4.2): `cancel` runs the same cascade as `cancel_task` from a registry
this process owns; `takeover` / `takeover-attach` are the human-only takeover (`takeover.py`);
`resources` reads the resource reservation ledger and `resources-release` / `resources-repair` are its
human-only recovery commands (`resources.py`), as is `launch-settle` for a granted launch whose owner
died without leaving a task record; `run`
and `resume` fork a process that owns the new task until it settles (`detached.py`). `backends`
reports the registered backends, whether each binary is on PATH (`backends.is_installed`) and each
backend's declared capabilities (`backends.capability_metadata`, the same serialization as MCP
`list_backends` — including bounded extra-path support) — no
`--version` probe, no subprocess. Every command prints one Harflow ctl v1 JSON document
with `--json`; command payloads retain their existing fields. The separate version-info command
reports compatibility without importing the task runtime through the installed lazy entrypoint.
"""

from __future__ import annotations

from .workflow_definition_documents import WorkflowDefinitionDocumentError
from .workflow_receipt_documents import WorkflowReceiptDocumentError
from .workflow_run_documents import WorkflowRunDocumentError

from .state_ownership import state_session, StateOwnershipError

from .state_access import task_layout

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

from . import backends, control, detached, followups, identity, inbox, store, takeover
from . import tasks as tasks_module
from .tasks import default_log_dir

from .wire_contracts import CTL_FORMAT, WIRE_VERSION, envelope

CTL_JSON_VERSION = WIRE_VERSION

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
            print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "error": {"code": "usage", "message": message}})))
        print(f"{self.prog}: error: {message}", file=sys.stderr)
        raise SystemExit(2)


def _build_parser() -> tuple[_ArgumentParser, ...]:
    parser = _ArgumentParser(prog="harflow-ctl", epilog="State root: HARFLOW_STATE_ROOT, otherwise Path.home()/.harflow. Public task layout: <root>/tasks.")
    sub = parser.add_subparsers(dest="command", required=True, parser_class=_ArgumentParser)
    info = sub.add_parser("version-info", help="report side-effect-free Harflow v1 compatibility")
    info.add_argument("--json", action="store_true")

    for command in ("state-inspect", "state-recover"):
        recovery = sub.add_parser(command, help="inspect state without mutation" if command == "state-inspect" else "explicitly back up incompatible state and initialize a fresh store")
        recovery.add_argument("--json", action="store_true")
        if command == "state-recover":
            recovery.add_argument("--operation-id", required=True)
            recovery.add_argument("--confirmation-token", required=True)

    artifact_p = sub.add_parser('register-artifact', help='register an existing artifact metadata reference for the verified calling task', description='Register an artifact reference. Retry catalog-preparation errors to advance bounded caller indexing.')
    artifact_p.add_argument('path')
    artifact_p.add_argument('--kind', required=True, choices=('screenshot', 'log', 'report', 'other'))
    artifact_p.add_argument('--note', default='')
    artifact_p.add_argument('--json', action='store_true')
    artifacts_p = sub.add_parser('task-artifacts', help='page metadata references and current stat observations; does not verify content', description='Page artifact metadata. Retry catalog-preparation errors to advance bounded caller indexing.')
    artifacts_p.add_argument('task_id')
    artifacts_p.add_argument('--cursor')
    artifacts_p.add_argument('--limit', type=int, default=20)
    artifacts_p.add_argument('--json', action='store_true')
    notification_p = sub.add_parser('workflow-notifications', help='read root workflow subscriptions and up to 50 recent deliveries; direct callers only')
    notification_p.add_argument('workflow_run_id')
    notification_p.add_argument('--json', action='store_true')

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

    compact_p = sub.add_parser('task-list-compact', help='bounded filtered task headers for direct callers only (since is ISO8601)')
    compact_p.add_argument('--json', action='store_true')
    compact_p.add_argument('--limit', type=int, default=50)
    compact_p.add_argument('--cursor')
    compact_p.add_argument('--fields', help='comma-separated compact fields')
    for key in ('group', 'title-contains', 'repo', 'status', 'backend', 'since'):
        compact_p.add_argument('--' + key)

    backends_p = sub.add_parser(
        "backends", help="list the registered backends, PATH presence and declared capabilities"
    )
    backends_p.add_argument("--json", action="store_true")

    status_p = sub.add_parser("status", help="show one task's full state")
    status_p.add_argument("task_id")
    status_p.add_argument("--json", action="store_true")
    status_p.add_argument("--brief", action="store_true", help="bounded display projection")

    recovery_p = sub.add_parser("recovery-activity", help="bounded display-only backend transcript activity")
    recovery_p.add_argument("task_id")
    recovery_p.add_argument("--json", action="store_true")
    recovery_p.add_argument("--cursor")
    recovery_p.add_argument("--limit", type=int, default=50)

    status_batch_p = sub.add_parser(
        "status-batch", help="show several tasks' full state in one document (at most 100 ids)"
    )
    status_batch_p.add_argument(
        "--task-ids", required=True, help="comma-separated exact task ids, at most 100"
    )
    status_batch_p.add_argument("--json", action="store_true")

    send_p = sub.add_parser(
        "send",
        help="queue a message: for a live-input task's running agent, or for the next continuation "
             "of a task without live input (reports queued, never delivered)",
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

    resources_p = sub.add_parser(
        "resources",
        help="show each resource name's capacity, holders and queue from the reservation ledger",
    )
    resources_p.add_argument("--json", action="store_true")

    release_p = sub.add_parser(
        "resources-release",
        help="release one reservation whose owner cannot be settled automatically "
        "(for a person, never an agent)",
        description="Remove one resource reservation by id after a person has decided its owner is gone. "
        "A reservation that still has borrowers is flagged release_requested and goes with its last borrower.",
    )
    release_p.add_argument("--reservation-id", required=True)
    release_p.add_argument(
        "--confirm-uncertain", action="store_true", required=True,
        help="required: you have checked that the owner is finished or never started",
    )
    release_p.add_argument(
        "--also-release-session", action="store_true",
        help="also free the session claim of a granted launch that has no task record (the reservation's "
        "owner is asked nothing); launch-settle also settles its follow-ups and worktree usage",
    )
    release_p.add_argument("--json", action="store_true")

    repair_p = sub.add_parser(
        "resources-repair",
        help="move a corrupt resource ledger aside and start an empty one (for a person, never an agent)",
        description="Refuses a healthy ledger. The corrupt file is kept as ledger.corrupt-<timestamp>.json "
        "in the resources directory. Every resource operation refuses while the repair runs, then the holders "
        "and borrowers that unsettled task records and granted launches prove are restored in one step "
        "(run it again if it was interrupted). Waiting launches are not restored: check what is running "
        "before you start work that needs a named resource.",
    )
    repair_p.add_argument("--json", action="store_true")

    settle_p = sub.add_parser(
        "launch-settle",
        help="settle a launch whose owner died after it was granted and left no task record "
        "(for a person, never an agent)",
        description="For a launch that has no task record and whose owner is confirmed dead, after its resources or "
        "worktree were granted: whether the agent started cannot be known. Releases the launch's reservations, its "
        "session claim and its worktree usage, and marks queued follow-ups delivery_unknown (back in the queue only "
        "when the agent provably never started). Run it only after checking that no agent for this task is running.")
    settle_p.add_argument("--task-id", required=True)
    settle_p.add_argument(
        "--confirm-uncertain", action="store_true", required=True,
        help="required: you have checked that no agent for this task is running")
    settle_p.add_argument("--json", action="store_true")

    worktree_list_p = sub.add_parser("worktree-list", help="list the task worktrees Harflow created, with their users")
    worktree_list_p.add_argument("--json", action="store_true")

    worktree_remove_p = sub.add_parser(
        "worktree-remove",
        help="remove one task's worktree when nothing uses it and it is clean (for a person, never an agent)",
        description="Refuses while any task or launch using the worktree is unsettled or uncertain, and while the "
        "tree has uncommitted or untracked changes. Never forces, never deletes the branch.")
    worktree_remove_p.add_argument("--task-id", required=True)
    worktree_remove_p.add_argument("--json", action="store_true")

    scratch_p = sub.add_parser(
        "scratch",
        help="list or clean the shared chain and group scratch folders",
        description="Shared scratch folders outlive a task: one per resume/continuation chain and one per group, "
        "under <state root>/shared-scratch. Nothing removes one on task completion, restart or recovery; "
        "only `scratch clean` and the HARFLOW_SHARED_SCRATCH_DAYS age policy do, and only when nothing references it unsettled.")
    scratch_sub = scratch_p.add_subparsers(dest="scratch_command", required=True, parser_class=_ArgumentParser)
    scratch_list_p = scratch_sub.add_parser("list", help="list shared folders with exact identity, size, last use and active users")
    scratch_list_p.add_argument("--json", action="store_true")
    scratch_clean_p = scratch_sub.add_parser(
        "clean", help="remove one shared folder when nothing uses it (for a person, never an agent)",
        description="Refuses while any task, launch or continuation that uses the folder is unsettled or uncertain, "
        "while a referencing record is pinned, and while a retained record holds a registered artifact under it.")
    scratch_target = scratch_clean_p.add_mutually_exclusive_group(required=True)
    scratch_target.add_argument("--group", help="the exact group name")
    scratch_target.add_argument("--chain", help="the chain id (shown by `scratch list`; the first task's id)")
    scratch_clean_p.add_argument("--json", action="store_true")

    approval_list = sub.add_parser("approval-list", help="List permission request metadata")
    approval_target = approval_list.add_mutually_exclusive_group(required=True)
    approval_target.add_argument("--task-id")
    approval_target.add_argument("--workflow-run-id")
    approval_list.add_argument("--cursor")
    approval_list.add_argument("--limit", type=int, default=16)
    approval_list.add_argument("--json", action="store_true")
    for command in ("approval-get", "approval-respond"):
        approval = sub.add_parser(command, help="Inspect or answer one pending exact-action permission request (an expired or denied one is recovered by a new resume or continue, not by answering)")
        approval.add_argument("task_id")
        approval.add_argument("request_id")
        approval.add_argument("--monitor", action="store_true", help="Use Harflow as the interaction channel")
        approval.add_argument("--json", action="store_true")
        if command == "approval-respond":
            approval.add_argument("--action-digest", required=True)
            approval.add_argument("--owner-revision", type=int, required=True)
            approval.add_argument("--decision", choices=("approve", "reject"), required=True)

    run_p = sub.add_parser("run", help="start a task, owned by a detached process until it settles")
    run_p.add_argument("--caller-approvals", action="store_true", default=None, help="Opt in to qualified exact-action caller decisions (Bash, and Read on measured versions)")
    run_p.add_argument("--caller-write-approvals", action="store_true", default=None, help="Also allow Write and Edit requests to be approved one exact action at a time")
    run_p.add_argument("--backend", default=None)
    run_p.add_argument("--workflow", default=None)
    run_goal = run_p.add_mutually_exclusive_group()
    run_goal.add_argument("--goal", help="Optional goal when --workflow is selected")
    run_goal.add_argument("--goal-file", help="UTF-8 goal file when --workflow is selected")
    run_p.add_argument("--monitor", action="store_true", help="Use Harflow as the workflow interaction channel")
    run_p.add_argument("--repo", required=True)
    run_p.add_argument("--freedom", default=None)
    run_p.add_argument("--prompt", required=True)
    run_p.add_argument("--model", default=None)
    run_p.add_argument("--max-turns", type=int, default=None)
    run_p.add_argument("--background-idle-seconds", type=float, help="live-input background wait bound (0 < seconds <= 21600)")
    run_p.add_argument("--auto-continue", type=int, default=0, help="0–10 fresh successors after certified cutoffs; standalone tasks only")
    run_p.add_argument("--reasoning-effort", default=None)
    run_p.add_argument("--network", choices=("true", "false"), default=None)
    run_p.add_argument("--extra-writable-path", action="append", default=None,
                       help="Existing absolute file/directory grant; repeat up to 16 times; verified harnesses only")
    run_p.add_argument("--allowed-command", action="append", default=None,
                       help="Bash command pattern granted for this run only; repeat up to 16 times. Claude Code at write_in_repo or publish only; see docs/harness-permissions.md")
    run_p.add_argument("--resource", action="append", default=None, metavar="NAME",
                       help="exclusive named resource the task needs before it starts (repeatable, at most 16); "
                            "the task waits in the background while another run holds it. Not for --workflow runs")
    run_p.add_argument("--resource-wait-seconds", type=int, default=None,
                       help="how long to wait for --resource names, 0..21600 (default 3600; 0 = grant now or fail)")
    run_p.add_argument("--worktree", action="store_true",
                       help="run the task in its own linked Git worktree under HARFLOW_WORKTREE_ROOT (default ~/.harflow-worktrees); "
                            "refused at read_only and for workflow runs")
    run_p.add_argument("--worktree-base", default=None, metavar="REF", help="commit-ish the worktree starts from (default HEAD)")
    worktree_target = run_p.add_mutually_exclusive_group()
    worktree_target.add_argument("--worktree-branch", default=None, metavar="NAME", help="new branch for the worktree (default harflow/<task id>)")
    worktree_target.add_argument("--worktree-detach", action="store_true", help="check the commit out detached, with no branch")
    run_p.add_argument("--group", default=None)
    run_p.add_argument("--title", default=None)
    run_p.add_argument("--json", action="store_true")

    preview_p = sub.add_parser("permission-preview", help="validate and preview standalone permissions without launching")
    preview_p.add_argument("--caller-approvals", action="store_true")
    preview_p.add_argument("--caller-write-approvals", action="store_true")
    preview_p.add_argument("--repo", required=True)
    preview_p.add_argument("--backend", required=True)
    preview_p.add_argument("--freedom", default=None)
    preview_p.add_argument("--network", choices=("true", "false"), default=None)
    preview_p.add_argument("--extra-writable-path", action="append", default=None,
                           help="Existing absolute file/directory grant; repeat up to 16 times")
    preview_p.add_argument("--allowed-command", action="append", default=None,
                           help="Bash command pattern granted for this run only; repeat up to 16 times. Claude Code at write_in_repo or publish only; see docs/harness-permissions.md")
    preview_p.add_argument("--json", action="store_true")

    resume_p = sub.add_parser(
        "resume",
        help="continue a finished task's session, owned by a detached process; follow-ups queued "
             "with `send` are consumed by it, so text may be omitted only when something is queued",
    )
    resume_p.add_argument("task_id")
    resume_p.add_argument("text", nargs="?", default=None)
    resume_p.add_argument("--max-turns", type=int, default=None)
    resume_p.add_argument("--background-idle-seconds", type=float)
    resume_p.add_argument("--network", choices=("true", "false"), default=None)
    resume_p.add_argument("--allowed-command", action="append", default=None,
                          help="Replace the saved command grants for this run (repeatable); omitted inherits them")
    resume_p.add_argument("--json", action="store_true")

    continue_p = sub.add_parser("continue", help="start a fresh session from a positively settled standalone task; follow-ups queued with `send` are consumed by it")
    continue_p.add_argument("task_id")
    extra = continue_p.add_mutually_exclusive_group()
    extra.add_argument("--extra-prompt")
    extra.add_argument("--extra-prompt-file", help="UTF-8 caller follow-up file")
    continue_p.add_argument("--background-idle-seconds", type=float)
    continue_p.add_argument("--notes-path", help="Optional UTF-8 notes file")
    continue_p.add_argument("--allowed-command", action="append", default=None,
                            help="Replace the saved command grants for the successor (repeatable); omitted inherits them")
    continue_p.add_argument("--json", action="store_true")

    allowlist_p = sub.add_parser("mcp-allowlist", help="inspect or explicitly edit harness-global MCP approval rules")
    allowlist_p.add_argument("--backend", required=True, choices=tuple(backends.BACKENDS))
    allowlist_p.add_argument("--json", action="store_true")
    edits = allowlist_p.add_mutually_exclusive_group()
    edits.add_argument("--allow")
    edits.add_argument("--remove")

    workflow_parsers = []
    for action in ("validate", "list", "list-page", "get", "save", "delete", "build", "preview", "start", "list-runs", "status", "detail", "wait", "pause", "resume", "cancel", "revise-goal", "builder-followup", "builder-apply", "inspect", "assigned-input", "artifacts", "recover", "migrate", "abandon-dispatch"):
        wp = sub.add_parser("workflow-" + action, help=action + " workflows")
        if action == "recover":
            wp.description = "Explicitly recover a failed settled run or an interrupted root with a confirmed-dead scheduler. Supply a reason; uncertain task ownership blocks recovery. Interrupted recovery preserves existing attempts and decision budgets."
        if action in {"save", "validate"}:
            wp.description = "Workflow nodes accept child_session_policy agent_decides (default), fresh, or resume in Child orchestrator mode. Resume requires the latest completed compatible invocation in the immediate parent run and never silently starts Fresh. Current mode retains its existing session owner."
        if action == "build":
            wp.description = "Generate or refine an unsaved workflow proposal. Preserve supplied Run workflow references and settings, including child_session_policy: agent_decides (default), fresh, or resume in Child mode; inactive in Current mode. Resume requires the latest completed compatible invocation in the immediate parent run with fresh workflow state and never silently starts Fresh. Never author new Run workflow references."
        if action in {"build", "builder-followup"}:
            authoring = "Keep node and connection instructions concise and task-specific. HarflowMCP owns runtime retries, budget enforcement, configured fallback dispatch and user interaction; use existing configuration fields for limits and fallback candidates, and orchestrator needs_input for clarification or approval. End means intentional workflow termination, not waiting for user input. Preserve legitimate task branches and revision loops."
            wp.description = (wp.description + " " if wp.description else "") + authoring
        wp.add_argument("--json", action="store_true")
        if action in {"get", "save", "delete", "build", "preview", "start"}:
            wp.add_argument("name")
        if action in {"status", "detail", "wait", "pause", "resume", "cancel", "revise-goal", "builder-followup", "inspect", "assigned-input", "artifacts", "recover", "abandon-dispatch"}:
            wp.add_argument("workflow_run_id")
        if action == "list":
            wp.add_argument("--monitor-view", action="store_true")
        if action == "status":
            wp.add_argument("--monitor-view", action="store_true")
            wp.add_argument("--snapshot", action="store_true")
            wp.add_argument("--cursor")
        if action == "detail":
            wp.add_argument("--monitor-view", action="store_true")
            wp.add_argument("--view", required=True, help="Lossless detail view, including on_demand_nodes, on_demand_calls or on_demand_state")
            wp.add_argument("--cursor")
        if action == "abandon-dispatch":
            wp.add_argument("execution_id")
            wp.add_argument("task_id")
            wp.add_argument("--reason", required=True)
            wp.add_argument("--confirm-no-process", action="store_true", required=True)
        if action == "assigned-input":
            wp.add_argument("execution_id")
            wp.add_argument("--source-run", dest="source_run_id")
            wp.add_argument("--task-id")
            wp.add_argument("--cursor")
            wp.add_argument("--limit", type=int, default=16000)
        if action == "artifacts":
            wp.description = "Page artifact references (metadata only) registered by the attempts of a workflow run and its linked child runs. A verified human reads any run; a delegation orchestrator reads its own run and linked descendants."
            wp.add_argument("--execution-id")
            wp.add_argument("--task-id")
            wp.add_argument("--cursor")
            wp.add_argument("--limit", type=int, default=20)
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
        if action in {"start", "pause", "resume", "recover", "cancel", "revise-goal"}:
            wp.add_argument("--monitor", action="store_true", help="Use Harflow as the workflow interaction channel")
        if action in {"start", "revise-goal"}:
            goal_group = wp.add_mutually_exclusive_group(required=action == "revise-goal")
            goal_group.add_argument("--goal", help="Verbatim nonblank run goal, up to 64 KiB UTF-8")
            goal_group.add_argument("--goal-file", help="UTF-8 run goal file, preserving spaces and line breaks")
        if action == "revise-goal":
            wp.add_argument("--expected-revision", type=int, required=True, help="Current goal revision, or 0 when absent; history is limited to 64 revisions and 1 MiB UTF-8 per run")
        if action == "resume":
            wp.add_argument("--decision-id")
            wp.add_argument("--allow-optional-review-skip", action="store_true", help="Explicitly permit skipping the current eligible optional review refusal; requires its current decision ID and answer")
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
                wp.add_argument("--expected-workflow-id", help="Loaded stable workflow identity; rejects deleted or replaced definitions")
        if action in {"start", "build", "preview"}:
            wp.add_argument("--repo", required=action in {"start", "preview"})
            prompt_group = wp.add_mutually_exclusive_group(required=action != "preview")
            prompt_group.add_argument("--prompt")
            prompt_group.add_argument("--prompt-file", help="UTF-8 prompt file")
            wp.add_argument("--backend", required=action == "build")
            wp.add_argument("--caller-approvals", action="store_true", default=None)
            wp.add_argument("--caller-write-approvals", action="store_true", default=None)
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
                if action == "start":
                    wp.add_argument("--freedom", default=None, choices=("read_only", "write_in_repo", "publish", "unrestricted"))
                wp.add_argument("--network", choices=("true", "false"))
                if action == "start":
                    wp.add_argument("--expected-preview-hash")
        if action == "wait":
            wp.add_argument("--timeout-seconds", type=int, default=None,
                            help="hold for 0..3600 seconds (default 30; omit with --until terminal)")
            wp.add_argument("--until", choices=("timeout", "terminal"), default="timeout",
                            help="'terminal' holds until the run settles or needs attention, "
                                 "up to a 6 hour server ceiling")
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
        status_batch_p,
        send_p,
        cancel_p,
        takeover_p,
        attach_p,
        resources_p,
        release_p,
        repair_p,
        settle_p,
        worktree_list_p,
        worktree_remove_p,
        scratch_p,
        scratch_list_p,
        scratch_clean_p,
        run_p,
        resume_p,
        allowlist_p,
        artifact_p,
        artifacts_p,
        notification_p,
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
    scanned = store.scan(log_dir)
    records = scanned.records

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
                    f"harflow-ctl: skipping {record.task_id}: unparsable started_at",
                    file=sys.stderr,
                )
                continue
            if started >= cutoff:
                kept.append(record)
        records = kept

    from .workflow_inspection import decorate_tasks
    entries = []
    for record in records:
        try:
            entries.append(store.brief(log_dir, record))
        except store.TaskDocumentError as exc:
            scanned.refusals[record.task_id] = exc
    entries = decorate_tasks(entries, log_dir)
    from .workflow_inspection import filter_task_reads, managed_reader
    try:
        entries = filter_task_reads(entries, managed_reader(log_dir))
    except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
        raise
    except Exception as exc:
        return _fail(args, "workflow_read_refused", str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "tasks": entries, **scanned.diagnostics()})))
    else:
        _print_table(entries)
        if scanned.history_incomplete:
            print("harflow-ctl: task history incomplete: " + ", ".join(scanned.refusals), file=sys.stderr)
    return 0


def _cmd_backends(args: argparse.Namespace) -> int:
    entries = [
        {
            "backend": backend.name,
            "binary": backend.binary,
            "installed": backends.is_installed(backend),
            "capabilities": backends.capability_metadata(backend),
        }
        for backend in backends.BACKENDS.values()
    ]
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "backends": entries})))
    else:
        for entry in entries:
            state = "installed" if entry["installed"] else "not found on PATH"
            print(f"{entry['backend']}  {state}")
    return 0


class _StatusReadRefused(Exception):
    """One task id's share of a read: the same code and message single `status` reports."""

    def __init__(self, code: str, message: str, data: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


def _status_snapshot(log_dir: Path, task_id: str, managed: Any) -> dict[str, Any]:
    """One task's full snapshot — the shared body of `status` and `status-batch`.

    Raises `_StatusReadRefused` with the same `code`/`message` a single `status` reports for this
    id, so a batch entry and a direct status call can never disagree. `task_id` must already be
    validated (invariant 8: an id is validated before it can become a path).
    """
    record = store.read(log_dir, task_id)
    from .workflow_inspection import guard_task_read
    try:
        guard_task_read(task_id, managed)
    except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
        raise
    except Exception as exc:
        raise _StatusReadRefused("workflow_read_refused", str(exc)) from None
    if record is None:
        from . import task_resources
        from .task_supervision import read_launch
        receipt = read_launch(log_dir, task_id)
        if receipt is not None and task_resources.is_pending_kind(receipt) and receipt["state"] not in {"spawned", "settled"}:
            # Not a task yet: its owner holds the wait. An older consumer treats the code as unknown.
            raise _StatusReadRefused(
                "task_pending_launch",
                f"task {task_id} has not started: its launch is {receipt['state']}; no task record exists until it spawns",
                {"launch_state": receipt["state"], **({"not_started_reason": receipt["not_started_reason"]} if receipt.get("not_started_reason") else {})})
        raise _StatusReadRefused("unknown_task", f"unknown task_id: {task_id}")
    from .workflow_inspection import decorate_tasks
    return decorate_tasks([store.snapshot(log_dir, record)], log_dir)[0]


def _cmd_status(args: argparse.Namespace) -> int:
    log_dir = default_log_dir()
    try:
        task_id = store.validate_task_id(args.task_id)
    except store.InvalidTaskId:
        return _fail(args, "invalid_task_id", f"not a valid task id: {args.task_id!r}")

    # Task qualification precedes workflow guards so direct errors retain their code.
    store.read(log_dir, task_id, include_prompt=False)
    from .workflow_inspection import managed_reader
    try:
        managed = managed_reader(log_dir)
    except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
        raise
    except Exception as exc:
        return _fail(args, "workflow_read_refused", str(exc))

    try:
        snapshot = _status_snapshot(log_dir, task_id, managed)
    except _StatusReadRefused as exc:
        return _fail(args, exc.code, exc.message, exc.data)

    if getattr(args, 'brief', False):
        from .task_reads import brief_status
        from .events import events_path, read_recent
        snapshot['recent_activity'] = read_recent(events_path(log_dir, task_id), limit=3, layout=task_layout(log_dir))
        snapshot = brief_status(snapshot)
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "task": snapshot})))
    else:
        for key, value in snapshot.items():
            print(f"{key}: {value}")
    return 0


# One `status-batch` bounds both the work and the document: the Monitor polls leased tasks
# through it, and 100 snapshots is already a large response.
STATUS_BATCH_MAX_IDS = 100


def _cmd_status_batch(args: argparse.Namespace) -> int:
    log_dir = default_log_dir()
    ids = list(dict.fromkeys(args.task_ids.split(",")))
    if not 1 <= len(ids) <= STATUS_BATCH_MAX_IDS:
        return _fail(
            args,
            "invalid_params",
            f"status-batch takes 1..{STATUS_BATCH_MAX_IDS} comma-separated task ids, got {len(ids)}",
        )
    # Invariant 8: every id is validated before any of them can become a path.
    valid: list[str] = []
    errors: dict[str, dict[str, str]] = {}
    for raw in ids:
        try:
            valid.append(store.validate_task_id(raw))
        except store.InvalidTaskId:
            errors[raw] = {"code": "invalid_task_id", "message": f"not a valid task id: {raw!r}"}

    from .workflow_inspection import managed_reader
    try:
        managed = managed_reader(log_dir)
    except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
        raise
    except Exception as exc:
        # Undecidable caller authority refuses the whole batch — never partial data.
        return _fail(args, "workflow_read_refused", str(exc))

    tasks: list[dict[str, Any]] = []
    for task_id in valid:
        try:
            tasks.append(_status_snapshot(log_dir, task_id, managed))
        except store.TaskDocumentError as exc:
            errors[task_id] = {"code": exc.code, "message": str(exc)}
        except _StatusReadRefused as exc:
            errors[task_id] = {"code": exc.code, "message": exc.message, **({"data": exc.data} if exc.data else {})}

    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "tasks": tasks, "errors": errors})))
    else:
        for snapshot in tasks:
            print(f"{snapshot.get('task_id')}: {snapshot.get('status')}")
        for error in errors.values():
            print(f"harflow-ctl: error: {error['message']}", file=sys.stderr)
    return 0


def _cmd_send(args: argparse.Namespace) -> int:
    def fail(code: str, message: str) -> int:
        print(f"harflow-ctl: error: {message}", file=sys.stderr)
        if args.json:
            print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "error": {"code": code, "message": message}})))
        return 1

    try:
        task_id = store.validate_task_id(args.task_id)
    except store.InvalidTaskId:
        return fail("invalid_task_id", f"not a valid task id: {args.task_id!r}")
    if not args.text.strip():
        return fail("empty_text", "text must be a non-empty string")

    try:
        by = identity.own_identity()
    except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
        raise
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
        result = followups.dispatch_send(default_log_dir(), task_id, args.text, by=by)
    except inbox.SendRefused as exc:
        return fail(exc.code, str(exc))

    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    elif result.get("delivery") == "next_continuation":
        print(f"queued {result['message_id']} for {task_id} (next continuation; nothing started or delivered)")
    else:
        print(f"queued {result['message_id']} for {task_id} (not yet delivered)")
    return 0


def _fail(args: argparse.Namespace, code: str, message: str, data: Any = None) -> int:
    print(f"harflow-ctl: error: {message}", file=sys.stderr)
    if getattr(args, "json", False):
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "error": {"code": code, "message": message, **({"data": data} if data is not None else {})}})))
    return 1


def _cmd_cancel(args: argparse.Namespace) -> int:
    try:
        task_id = store.validate_task_id(args.task_id)
    except store.InvalidTaskId:
        return _fail(args, "invalid_task_id", f"not a valid task id: {args.task_id!r}")
    log_dir = default_log_dir()
    if store.read(log_dir, task_id) is None:
        from . import task_resources
        pending = task_resources.pending_view(log_dir, task_id)
        if pending is not None:
            return _cancel_pending(args, log_dir, task_id)
        return _fail(args, "unknown_task", f"unknown task_id: {task_id}")

    registry = tasks_module.TaskRegistry(log_dir=log_dir, open_monitor=False, execution_mode="supervised")

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
            "harflow-ctl: warning: signals were delivered but could not all be recorded: "
            + "; ".join(unrecorded),
            file=sys.stderr,
        )
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    else:
        print(f"{task_id}: {status}")
    return 0


def _cancel_pending(args: argparse.Namespace, log_dir: Path, task_id: str) -> int:
    """`cancel` for a launch with no record: ask its owner to stop waiting, in the existing result shape."""
    from . import task_resources
    registry = tasks_module.TaskRegistry(log_dir=log_dir, open_monitor=False, execution_mode="supervised")
    try:
        outcome = asyncio.run(registry.cancel_pending(task_id))
    except (backends.UnsupportedCapability, ValueError) as exc:
        return _fail(args, "invalid_params", str(exc))
    if outcome is None:
        return _fail(args, "unknown_task", f"unknown task_id: {task_id}")
    if outcome["spawned"]:
        return _cmd_cancel(args)  # it spawned while we asked: the ordinary cancel takes over
    view = task_resources.pending_view(log_dir, task_id)
    status = view["status"] if view is not None else "cancelled"
    result = {"task_id": task_id, "status": status, "cascade": outcome["cascade"]}
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    else:
        print(f"{task_id}: {status} (cancel requested before the task started)")
    return 0


def _cmd_takeover(args: argparse.Namespace) -> int:
    log_dir = default_log_dir()
    try:
        result = asyncio.run(
            takeover.take_over(
                log_dir,
                args.task_id,
                registry_factory=lambda: tasks_module.TaskRegistry(
                    log_dir=log_dir, open_monitor=False, execution_mode="supervised"
                ),
            )
        )
    except control.TakeoverRefused as exc:
        return _fail(args, exc.code, str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
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
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    else:
        print(f"attached pid {result['pid']} to the takeover of {result['task_id']}")
    return 0


def _cmd_resources(args: argparse.Namespace) -> int:
    from . import resources

    try:
        entries = resources.ResourceLedger(default_log_dir().parent).snapshot()
    except resources.ResourceError as exc:
        return _fail(args, exc.code, str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "resources": entries})))
        return 0
    if not entries:
        print("no resources reserved")
    for entry in entries:
        print(f"{entry['name']} (capacity {entry['capacity']}): {len(entry['holders'])} held, {len(entry['queue'])} queued")
        for kind in ("holders", "queue"):
            for row in entry[kind]:
                print(f"  {kind[:-1]:6} {row['reservation_id']}  {json.dumps(row['owner'], sort_keys=True)}")
    return 0


def _human_only_refusal(args: argparse.Namespace, what: str) -> int | None:
    """The takeover gate for the recovery commands: an agent, or detection that cannot rule one out,
    must not free a resource another run may still be using."""
    refusal = takeover.caller_refusal(default_log_dir())
    if refusal is None:
        return None
    code, reason = refusal
    return _fail(args, code, f"Only a verified human may {what}: {reason}")


def _cmd_resources_release(args: argparse.Namespace) -> int:
    from . import resources, task_resources

    refused = _human_only_refusal(args, "release a resource reservation")
    if refused is not None:
        return refused
    log_dir = default_log_dir()
    try:
        reservation_id = resources.validate_reservation_id(args.reservation_id)
        ledger = resources.ResourceLedger(log_dir.parent)
        entry = ledger.get(reservation_id)
        outcome = ledger.release(reservation_id)
    except resources.ResourceError as exc:
        return _fail(args, exc.code, str(exc))
    if outcome == "absent":
        return _fail(args, "unknown_reservation", f"unknown reservation {reservation_id}")
    session = "not_requested"
    if args.also_release_session:
        # Only a `granted` launch with no record holds a session claim the ledger cannot see; a
        # running task's claim is its live process, which a person must stop, not release.
        task_id = entry.owner.task_id if entry is not None else None
        session = task_resources.release_session_claim(log_dir, task_id) if task_id else "not_applicable"
    result = {
        "reservation_id": reservation_id,
        "outcome": outcome,
        "also_release_session": bool(args.also_release_session),
        "session_released": session == "released",
        **({"session": session} if args.also_release_session else {}),
    }
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    elif outcome == "release_requested":
        print(f"{reservation_id}: release requested; it goes when its last borrower is released")
    else:
        print(f"{reservation_id}: released" + ("; session claim released" if session == "released" else ""))
    return 0


def _cmd_resources_repair(args: argparse.Namespace) -> int:
    from . import resources, task_resources

    refused = _human_only_refusal(args, "repair the resource ledger")
    if refused is not None:
        return refused
    log_dir = default_log_dir()
    try:
        rebuilt = task_resources.repair(log_dir)
    except resources.ResourceError as exc:
        return _fail(args, exc.code, str(exc))
    moved_to = rebuilt["moved_to"]
    result = {
        "moved_to": moved_to,
        "rebuilt": True,
        "restored": rebuilt["restored"],
        "conflicts": rebuilt["conflicts"],
        "note": "The ledger was published in one step from durable evidence only: holders of unsettled task records "
        "and granted launches were restored together with the borrowers that depend on them (a lender that cannot "
        "be found is replaced by an entry that blocks its borrowers' names until they finish or a person releases "
        "it); waiting launches and anything unprovable were not. Verify what is running before starting work that "
        "needs a named resource.",
    }
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    else:
        print(f"moved the corrupt ledger to {moved_to}; restored {len(rebuilt['restored'])} entr{'y' if len(rebuilt['restored']) == 1 else 'ies'}")
        print(result["note"], file=sys.stderr)
    return 0


def _cmd_launch_settle(args: argparse.Namespace) -> int:
    from . import task_resources

    refused = _human_only_refusal(args, "settle an uncertain launch")
    if refused is not None:
        return refused
    try:
        task_id = store.validate_task_id(args.task_id)
        result = task_resources.settle_uncertain(default_log_dir(), task_id)
    except store.InvalidTaskId:
        return _fail(args, "invalid_task_id", f"not a valid task id: {args.task_id!r}")
    except task_resources.LaunchSettleRefused as exc:
        return _fail(args, exc.code, str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    else:
        print(f"{task_id}: settled ({result['outcome']}); released {len(result['released_reservations'])} reservation(s)")
    return 0


def _cmd_worktree_list(args: argparse.Namespace) -> int:
    from . import worktrees
    try:
        entries = worktrees.inventory(default_log_dir())
    except (worktrees.WorktreeError, OSError) as exc:
        return _fail(args, getattr(exc, "code", "worktree_error"), str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "worktrees": entries})))
        return 0
    if not entries:
        print("no task worktrees")
    for entry in entries:
        users = ", ".join(f"{u['task_id']} ({u['status']})" for u in entry["users"]) or "unused"
        print(f"{entry['task_id']}  {entry['path']}  {'exists' if entry['exists'] else 'missing'}  {users}")
    return 0


def _cmd_worktree_remove(args: argparse.Namespace) -> int:
    from . import worktrees
    refused = _human_only_refusal(args, "remove a task worktree")
    if refused is not None:
        return refused
    try:
        task_id = store.validate_task_id(args.task_id)
        result = worktrees.remove(default_log_dir(), task_id)
    except store.InvalidTaskId:
        return _fail(args, "invalid_task_id", f"not a valid task id: {args.task_id!r}")
    except worktrees.WorktreeError as exc:
        return _fail(args, exc.code, str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    else:
        print(f"removed {result['path']}; {result['note']}")
    return 0


def _cmd_scratch_list(args: argparse.Namespace) -> int:
    from . import shared_scratch
    try:
        entries = shared_scratch.inventory(default_log_dir())
    except (shared_scratch.SharedScratchError, store.TaskDocumentError, OSError) as exc:
        return _fail(args, getattr(exc, "code", "shared_scratch_error"), str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "shared_scratch": entries})))
        return 0
    if not entries:
        print("no shared scratch folders")
    for entry in entries:
        size = f"{entry['bytes']}{'+' if entry['bytes_truncated'] else ''} bytes"
        print(f"{entry['kind']:5} {entry['identity'] if entry['identity'] is not None else '(unknown)'}  {entry['path']}  {size}  "
              f"last used {entry['last_used_at'] or 'unknown'}  {entry['active_tasks']} active")
    return 0


def _cmd_scratch_clean(args: argparse.Namespace) -> int:
    from . import shared_scratch
    refused = _human_only_refusal(args, "clean a shared scratch folder")
    if refused is not None:
        return refused
    kind, identity = ("group", args.group) if args.group is not None else ("chain", args.chain)
    try:
        result = shared_scratch.clean(default_log_dir(), kind, identity)
    except store.InvalidTaskId:
        return _fail(args, "invalid_chain_id", f"not a valid chain id: {identity!r}")
    except (shared_scratch.SharedScratchError, store.TaskDocumentError, OSError) as exc:
        return _fail(args, getattr(exc, "code", "shared_scratch_error"), str(exc))
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})))
    else:
        print(f"removed the {kind} folder {result['path']}; {result['note']}")
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
        server._check_allowed_commands(chosen, freedom, path, getattr(args, 'allowed_command', None))
        return await registry.start(
            args.prompt,
            path,
            backend=chosen,
            freedom=freedom,
            model=args.model,
            max_turns=args.max_turns, **({'background_idle_seconds': args.background_idle_seconds} if getattr(args, 'background_idle_seconds', None) is not None else {}),
            reasoning_effort=args.reasoning_effort,
            network=network,
            group=args.group,
            title=title,
            auto_continue=args.auto_continue,
            caller_approvals=bool(args.caller_approvals),
            **({'caller_write_approvals': True} if getattr(args, 'caller_write_approvals', None) else {}),
            interaction_owner="monitor" if args.monitor else "caller",
            **({'extra_writable_paths': args.extra_writable_path} if args.extra_writable_path is not None else {}),
            **({'allowed_commands': getattr(args, 'allowed_command', None)} if getattr(args, 'allowed_command', None) is not None else {}),
            **({'resources': args.resource} if getattr(args, 'resource', None) is not None else {}),
            **({'worktree': {key: value for key, value in (('base_ref', args.worktree_base), ('branch', args.worktree_branch), ('detach', args.worktree_detach or None)) if value is not None}} if getattr(args, 'worktree', False) else {}),
            **({'resource_wait_seconds': args.resource_wait_seconds} if getattr(args, 'resource_wait_seconds', None) is not None else {}),
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
        if args.text is not None and not args.text.strip():
            raise MCPError(INVALID_PARAMS, "text must be a non-empty string when provided")
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
        if getattr(args, 'allowed_command', None) is not None:
            server._check_allowed_commands(backends.get(record.backend), record.freedom, Path(record.repo_path), args.allowed_command)
        return await registry.resume_record(
            record, args.text, max_turns=args.max_turns, **({'background_idle_seconds': args.background_idle_seconds} if getattr(args, 'background_idle_seconds', None) is not None else {}), network=network, consume_followups=True,
            **({'allowed_commands': getattr(args, 'allowed_command', None)} if getattr(args, 'allowed_command', None) is not None else {}),
        )

    return action


def _ctl_log_path() -> Path:
    return default_log_dir().parent / "ctl.log"


def _continue_action(args: argparse.Namespace):
    async def action(registry: Any) -> Any:
        if getattr(args, 'allowed_command', None) is not None:
            from . import backends, server
            source = await asyncio.to_thread(store.read, registry.log_dir, args.task_id, include_prompt=False)
            if source is not None:
                server._check_allowed_commands(backends.get(source.backend), source.freedom, Path(source.repo_path), args.allowed_command)
        return await registry.continue_task(args.task_id, _workflow_text(args, "extra_prompt"), args.notes_path, consume_followups=True, **({'background_idle_seconds': args.background_idle_seconds} if getattr(args, 'background_idle_seconds', None) is not None else {}), **({'allowed_commands': getattr(args, 'allowed_command', None)} if getattr(args, 'allowed_command', None) is not None else {}))
    return action


def _cmd_detached(args: argparse.Namespace, action: Any) -> int:
    log_dir = default_log_dir()
    outcome = detached.run_detached(
        action,
        log_path=_ctl_log_path(),
        log_dir=log_dir,
        # No retention and no open-app: the app drives these commands itself.
        registry_factory=lambda: tasks_module.TaskRegistry(log_dir=log_dir, open_monitor=False, execution_mode="supervised"),
    )
    if outcome.kind == "task":
        if args.json:
            print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": outcome.payload})))
        else:
            print(outcome.payload["task_id"])
        return 0
    if outcome.kind == "error":
        return _fail(
            args,
            str(outcome.payload.get("code", "start_failed")),
            str(outcome.payload.get("message", "")),
        )
    print(f"harflow-ctl: {outcome.payload['message']}", file=sys.stderr)
    if args.json:
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "unknown": outcome.payload})))
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
        if getattr(args, 'monitor_view', False) and action in {'list', 'status', 'detail'}:
            from .catalog import Catalog
            from .workflow_inspection import page_indexing_response
            directory = default_log_dir()
            if not Catalog(directory, store.RECORD_SUFFIX, layout=task_layout(directory)).ready():
                await asyncio.to_thread(store.bootstrap_catalog, directory)
                pending = page_indexing_response(directory)
                if action == 'list':
                    pending['workflows'] = []
                return pending
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
            diagnostics: dict[str, Any] = {}
            if any(node.get("type") == "workflow" for node in canonical.get("nodes", [])):
                # Resolution is additive: it reports the pinned tree or names the problem.
                from .workflow_references import DependencyError, resolve_dependencies
                from .workflows import WorkflowStore
                try:
                    tree = resolve_dependencies(WorkflowStore(default_log_dir().parent), definition=canonical, substitute_name=canonical.get("name"))
                    dependencies = {"root_workflow_id": tree["root_workflow_id"], "workflows": {ident: {"name": entry["name"], "revision": entry["revision"], "definition_sha256": entry["definition_sha256"]} for ident, entry in tree["workflows"].items()}, "edges": tree["edges"], "access": tree["access"]}
                except DependencyError as exc:
                    error = str(exc)
                    diagnostics = {"missing_references": exc.missing_references, "missing_references_truncated": exc.missing_references_truncated}
            from .backends.native import available as native_available
            from .workflow_definition_documents import definition_warnings
            return {**diagnostics, "warnings": definition_warnings(canonical), "valid": error is None, "native_subagents_available": native_available(), "definition": canonical, **({"dependencies": dependencies} if dependencies is not None else {}), **({"error": error} if error is not None else {})}
        if action in {"list", "list-runs", "list-page", "get", "status", "detail"}:
            from .read_metrics import span
            with span("import"):
                from .workflow_reads import cli_read
            return await cli_read(action, args, default_log_dir())
        if action in {"assigned-input", "inspect"}:
            # Node reads resolve their own managed authority and share the server's checks through
            # `workflow_node_reads`, so they never import the MCP server module (with its MCP
            # stack, retention startup and notification worker).
            from . import workflow_node_reads
            from .workflow_inspection import managed_reader
            managed = managed_reader(default_log_dir())
            try:
                if action == "assigned-input":
                    return await asyncio.to_thread(workflow_node_reads.assigned_input_node, managed, args.workflow_run_id, args.execution_id, source_run_id=args.source_run_id, task_id=args.task_id, cursor=args.cursor, limit=args.limit, root=default_log_dir().parent)
                return await asyncio.to_thread(workflow_node_reads.inspect_node, managed, args.workflow_run_id, execution_id=args.execution_id, task_id=args.task_id, view=args.view, cursor=args.cursor, limit=args.limit, before_seq=args.before_seq, after_seq=args.after_seq, root=default_log_dir().parent)
            except (ValueError, KeyError, OSError, backends.UnsupportedCapability) as exc:
                # Only the exception set the server path converts; anything else reaches the outer
                # handler as for other commands. The same message/data mapping the MCP path applies, so the CLI reports
                # the identical refusal. The MCP import stays on this failure path only.
                from mcp import MCPError
                from mcp.types import INVALID_PARAMS
                message, data = workflow_node_reads.error_payload(exc)
                raise MCPError(INVALID_PARAMS, message, data=data) from None
        if action == "artifacts":
            from . import workflow_node_reads
            from .takeover import caller_refusal
            from .workflow_inspection import managed_reader
            directory = default_log_dir()
            # The same authority as the MCP tool: a verified human reads any run, a delegation
            # orchestrator its own tree, and every other caller is refused.
            human = await asyncio.to_thread(caller_refusal, directory) is None
            managed = None if human else managed_reader(directory)
            return await asyncio.to_thread(workflow_node_reads.workflow_artifacts_page, managed, human, directory, args.workflow_run_id, cursor=args.cursor, limit=args.limit, execution_id=args.execution_id, task_id=args.task_id)
        if action == "abandon-dispatch":
            from .workflows import WorkflowStore
            from .takeover import caller_refusal
            refusal = await asyncio.to_thread(caller_refusal, default_log_dir())
            if refusal is not None:
                raise ValueError("Only a verified human may abandon an uncertain dispatch: " + refusal[1])
            return await asyncio.to_thread(WorkflowStore(default_log_dir().parent).abandon_dispatch, args.workflow_run_id, args.execution_id, args.task_id, args.reason, args.confirm_no_process)
        from . import server
        # Registry-backed helpers below get a registry without maintenance or notifications: ctl
        # has no MCP session, and its asyncio.run loop would cancel a notification worker anyway.
        server.install_cli_registry()
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
                refuse_managed(default_log_dir(), caller.record.task_id)
            return migrate_workflows()
        if action == "builder-followup":
            return await server.followup_workflow_builder(args.workflow_run_id, _workflow_prompt(args))
        if action == "builder-apply":
            raw = sys.stdin.read() if args.definition == "-" else Path(args.definition).read_text()
            return await server.apply_workflow_draft(json.loads(raw), args.expected_draft_revision)
        if action == "delete":
            return await server._workflow_call("delete", name=args.name)
        if action == "save":
            raw = sys.stdin.read() if args.definition == "-" else Path(args.definition).read_text()
            return await server.save_workflow(args.name, json.loads(raw), args.expected_revision, expected_workflow_id=args.expected_workflow_id)
        if action in {"start", "build", "preview"}:
            prompt = _workflow_prompt(args) if action != "preview" else ""
            candidate = {k: v for k, v in {"backend": args.backend, "model": args.model,
                         "reasoning_effort": args.reasoning_effort, "max_turns": args.max_turns, "background_idle_seconds": getattr(args, "background_idle_seconds", None), "caller_approvals": args.caller_approvals, "caller_write_approvals": getattr(args, "caller_write_approvals", None)}.items() if v is not None}
            if action == "preview":
                return await server._workflow_call("preview", name=args.name, repo_path=args.repo, overrides=candidate or None, network=_network(args.network))
            if action == "build":
                definition = json.loads(args.definition_json) if args.definition_json else None
                if args.definition:
                    definition = json.loads(sys.stdin.read() if args.definition == "-" else Path(args.definition).read_text())
                source = json.loads(args.source) if args.source else None
                if args.source_file:
                    source = json.loads(Path(args.source_file).read_text())
                return await server.workflow_builder(args.name, prompt, args.repo, candidate, json.loads(args.fallbacks), definition, source)
            preview_guard = {"expected_preview_hash": args.expected_preview_hash} if getattr(args, "expected_preview_hash", None) is not None else {}
            goal = _workflow_text(args, "goal")
            goal_guard = {"goal": goal} if goal is not None else {}
            if args.monitor:
                return await server._workflow_call("start", name=args.name, prompt=prompt, repo_path=args.repo, overrides=candidate or None, freedom=args.freedom, network=_network(args.network), interaction_owner="monitor", **preview_guard, **goal_guard)
            return await server._workflow_call("start", name=args.name, prompt=prompt, repo_path=args.repo, overrides=candidate or None, freedom=args.freedom, network=_network(args.network), **preview_guard, **goal_guard)
        if action == "revise-goal":
            return await server._workflow_call("revise_goal", run_id=args.workflow_run_id,
                goal=_workflow_text(args, "goal"), expected_revision=args.expected_revision,
                **({"interaction_owner": "monitor"} if args.monitor else {}))
        if action == "wait":
            return await server._wait_workflow_full(args.workflow_run_id, args.timeout_seconds, args.until)
        if action == "resume":
            instructions = _workflow_text(args, "instructions")
            if args.monitor:
                return await server._workflow_call("resume", run_id=args.workflow_run_id, instructions=instructions, additional_attempts=args.additional_attempts, decision_id=args.decision_id, allow_optional_review_skip=args.allow_optional_review_skip, interaction_owner="monitor")
            return await server._workflow_call("resume", run_id=args.workflow_run_id, instructions=instructions, additional_attempts=args.additional_attempts, decision_id=args.decision_id, allow_optional_review_skip=args.allow_optional_review_skip)
        return await server._workflow_call(action, run_id=args.workflow_run_id, **({"interaction_owner": "monitor"} if getattr(args, "monitor", False) else {}))
    try:
        result = asyncio.run(invoke())
    except (StateOwnershipError, store.TaskDocumentError, WorkflowDefinitionDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
        raise
    except Exception as exc:
        data = getattr(getattr(exc, "error", None), "data", None)
        code = data.get("code") if isinstance(data, dict) else None
        if code not in {"workflow_definition_incompatible", "workflow_definition_unavailable"}:
            code = "workflow_error"
        return _fail(args, code, str(exc), data)
    from .read_metrics import span
    with span("serialization"):
        output = json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})) if args.json else json.dumps(result, indent=2)
    print(output)
    return 0


def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "version-info":
        from .cli import version_info_main
        return version_info_main(arguments[1:])
    from .catalog import metadata_request
    with metadata_request():
        return _main(argv)


def _main(argv: list[str] | None = None) -> int:
    """Entry point for the `harflow-ctl` console script, which calls `sys.exit(main())`."""
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv and argv[0] == "version-info":
        from .cli import version_info_main
        return version_info_main(argv[1:])
    json_requested = "--json" in argv
    if "--help" not in argv and "-h" not in argv:
        from .state_layout import InvalidStateRoot, resolve_layout
        try:
            resolve_layout()
        except InvalidStateRoot as exc:
            parser = _ArgumentParser(prog="harflow-ctl")
            parser.json_requested = json_requested
            parser.error(str(exc))

    parsers = _build_parser()
    for each in parsers:
        each.json_requested = json_requested
    parser, list_p = parsers[0], parsers[1]

    args = parser.parse_args(argv)
    if args.command in {"state-inspect", "state-recover"}:
        from .state_layout import resolve_layout
        from .state_recovery import inspect_state, recover_state
        try:
            result = (inspect_state(resolve_layout()) if args.command == "state-inspect" else
                      recover_state(resolve_layout(), operation_id=args.operation_id,
                                    confirmation_token=args.confirmation_token))
        except StateOwnershipError as exc:
            return _fail(args, exc.code, str(exc), exc.as_dict())
        print(json.dumps(envelope(CTL_FORMAT, {"result": result})) if args.json else json.dumps(result, indent=2))
        return 0
    # Informational backend/config inspection does not access engine state.
    config_only = args.command == "backends" or (
        args.command == "mcp-allowlist" and args.allow is None and args.remove is None)
    if config_only:
        return _execute(args, list_p)
    try:
        with state_session(task_layout(default_log_dir())):
            return _execute(args, list_p)
    except (StateOwnershipError, store.TaskDocumentError, WorkflowDefinitionDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError) as exc:
        data = exc.as_dict() if isinstance(exc, (StateOwnershipError, WorkflowDefinitionDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError)) else ({"published": True, "workflow_run_id": exc.workflow_run_id} if getattr(exc, "published", False) else None)
        return _fail(args, getattr(exc, "code", "state_incompatible"), str(exc), data)


def _execute(args: argparse.Namespace, list_p: _ArgumentParser) -> int:
    mutation_commands = {"run", "resume", "continue", "send", "cancel", "takeover", "takeover-attach", "approval-respond", "register-artifact"}
    from .workflow_reads import READ_ONLY_CLI_COMMANDS
    if args.command in mutation_commands or args.command.startswith("workflow-") and args.command not in READ_ONLY_CLI_COMMANDS or args.command == "mcp-allowlist" and (args.allow is not None or args.remove is not None):
        from .caller_policy import require_mutation
        try:
            require_mutation(default_log_dir())
        except ValueError as exc:
            return _fail(args, "caller_undecidable", str(exc))

    if args.command in {"approval-list", "approval-get", "approval-respond"}:
        from . import approval_runtime, approvals, server
        registry = server.install_cli_registry()
        async def approval_action():
            reg = server._reg()
            if args.command == "approval-list":
                return await approval_runtime.list_requests(reg, args.task_id, args.workflow_run_id, args.cursor, args.limit)
            owner = "monitor" if args.monitor else "caller"
            if args.command == "approval-get":
                return await approval_runtime.get_request(reg, args.task_id, args.request_id, interaction_owner=owner)
            return await approval_runtime.respond_request(reg, args.task_id, args.request_id, args.action_digest, args.owner_revision, args.decision, owner)
        try:
            result = asyncio.run(approval_action())
        except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
            raise
        except approvals.ApprovalError as exc:
            return _fail(args, "approval_error", str(exc), {"reason": exc.reason})
        except Exception as exc:
            return _fail(args, "approval_error", str(exc))
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})) if args.json else json.dumps(result, indent=2))
        return 0

    if args.command == 'workflow-notifications':
        from .takeover import caller_refusal
        from .workflow_notifications import status
        try:
            if caller_refusal(default_log_dir()) is not None:
                raise ValueError('Workflow notifications require a direct caller')
            result = status(default_log_dir(), args.workflow_run_id)
        except (OSError, ValueError) as exc:
            return _fail(args, 'notification_error', str(exc))
        print(json.dumps(envelope(CTL_FORMAT, {'v': CTL_JSON_VERSION, 'result': result})) if args.json else json.dumps(result, indent=2))
        return 0

    if args.command in ('register-artifact', 'task-artifacts'):
        from . import artifacts
        try:
            result = (artifacts.register(default_log_dir(), args.path, args.kind, args.note)
                      if args.command == 'register-artifact' else
                      artifacts.get(default_log_dir(), args.task_id, args.cursor, args.limit))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            return _fail(args, 'artifact_error', str(exc))
        print(json.dumps(envelope(CTL_FORMAT, {'v': CTL_JSON_VERSION, 'result': result})) if args.json else json.dumps(result, indent=2))
        return 0

    if args.command == "permission-preview":
        from . import server
        # Registry-backed validation, without the maintenance sweep or notification worker a
        # live server's `_reg()` would start.
        server.install_cli_registry()
        try:
            result = asyncio.run(server.preview_task_permissions(
                repo_path=args.repo, backend=args.backend, freedom=args.freedom,
                network=_network(args.network), extra_writable_paths=args.extra_writable_path,
                caller_approvals=args.caller_approvals,
                **({"caller_write_approvals": True} if args.caller_write_approvals else {}),
                **({"allowed_commands": getattr(args, 'allowed_command', None)} if getattr(args, 'allowed_command', None) is not None else {}),
            ))
        except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
            raise
        except Exception as exc:
            return _fail(args, "invalid_params", str(exc))
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})) if args.json else json.dumps(result, indent=2))
        return 0

    if args.command == "mcp-allowlist":
        from . import mcp_allowlist
        try:
            if args.allow is not None or args.remove is not None:
                from .takeover import caller_refusal
                refusal = caller_refusal(default_log_dir())
                if refusal is not None:
                    return _fail(args, "human_only", "Only a verified human may change global MCP approval rules: " + refusal[1])
            result = mcp_allowlist.edit(args.backend, allow=args.allow, remove=args.remove)
            print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "result": result})) if args.json else result["detail"])
            return 0
        except (ValueError, OSError, TypeError, AttributeError) as exc:
            return _fail(args, "allowlist_error", str(exc))
    if args.command.startswith("workflow-"):
        return _cmd_workflow(args)
    if args.command == "list":
        return _cmd_list(args, list_p)
    if args.command == 'task-list-compact':
        try:
            from .task_reads import list_tasks
            from .workflow_inspection import managed_page_reader
            ready, managed = managed_page_reader(default_log_dir())
            if not ready:
                from .workflow_inspection import page_indexing_response
                result = page_indexing_response(default_log_dir())
            else:
                if managed is not None:
                    raise ValueError('Compact task listing is available only to direct callers; managed callers should use task-list-page --task-ids with known task IDs or status reads.')
                result = list_tasks(default_log_dir(), limit=args.limit, cursor=args.cursor, fields=args.fields.split(',') if args.fields else None, **{key: getattr(args, key) for key in ('group', 'title_contains', 'repo', 'status', 'backend', 'since')})
            print(json.dumps(envelope(CTL_FORMAT, {'v': CTL_JSON_VERSION, 'result': result})) if args.json else json.dumps(result, indent=2))
            return 0
        except (ValueError, OSError) as exc:
            return _fail(args, 'invalid_params', str(exc))
    if args.command == 'task-list-page':
        try:
            from .workflow_inspection import filter_task_reads, managed_page_reader
            ready, managed = managed_page_reader(default_log_dir())
            if not ready:
                from .workflow_inspection import page_indexing_response
                result = page_indexing_response(default_log_dir())
                print(json.dumps(envelope(CTL_FORMAT, {'v': CTL_JSON_VERSION, 'result': result})) if args.json else json.dumps(result, indent=2))
                return 0
            result = store.list_page(default_log_dir(), limit=args.limit, cursor=args.cursor, active_only=args.active_only, session_id=args.session_id, task_ids=args.task_ids.split(',') if args.task_ids is not None else None)
            result['items'] = filter_task_reads(result['items'], managed)
            result['related_headers'] = filter_task_reads(result.get('related_headers', []), managed)
            if managed is not None:
                result['total_active_count'] = None if result.get('bootstrap_pending') or result.get('counts_complete') is False else sum(item.get('status') not in store.TERMINAL_RECORD_STATUSES for item in result['items'])
                result.pop('total_active_root_count', None)
                result.pop('total_attention_root_count', None)
                result.update(next_cursor=None, has_more=False)
            print(json.dumps(envelope(CTL_FORMAT, {'v': CTL_JSON_VERSION, 'result': result})) if args.json else json.dumps(result, indent=2))
            return 0
        except (ValueError, OSError) as exc:
            return _fail(args, 'invalid_params', str(exc))
    if args.command == "backends":
        return _cmd_backends(args)
    if args.command == "send":
        return _cmd_send(args)
    if args.command == "worktree-list":
        return _cmd_worktree_list(args)
    if args.command == "worktree-remove":
        return _cmd_worktree_remove(args)
    if args.command == "scratch":
        return _cmd_scratch_list(args) if args.scratch_command == "list" else _cmd_scratch_clean(args)
    if args.command == "resources":
        return _cmd_resources(args)
    if args.command == "resources-release":
        return _cmd_resources_release(args)
    if args.command == "resources-repair":
        return _cmd_resources_repair(args)
    if args.command == "launch-settle":
        return _cmd_launch_settle(args)
    if args.command == "cancel":
        return _cmd_cancel(args)
    if args.command == "takeover":
        return _cmd_takeover(args)
    if args.command == "takeover-attach":
        return _cmd_takeover_attach(args)
    if args.command == "run":
        if args.workflow:
            if args.extra_writable_path is not None:
                return _fail(args, "invalid_params", "Extra writable paths are unavailable for workflow launches")
            if getattr(args, 'allowed_command', None) is not None:
                return _fail(args, "invalid_params", "allowed_commands is unavailable for workflow launches; grant commands on saved workflow agent candidates instead")
            if args.auto_continue:
                return _fail(args, "invalid_params", "Workflow retries belong to their supervisor; --auto-continue is unavailable")
            if args.group is not None or args.title is not None:
                return _fail(args, "invalid_params", "workflow runs do not accept task group or title")
            if args.resource is not None or args.resource_wait_seconds is not None:
                return _fail(args, "invalid_params", "resources are unavailable for workflow launches")
            if args.worktree:
                return _fail(args, "invalid_params", "worktree is unavailable for workflow launches")
            args.name = args.workflow
            args.command = "workflow-start"
            return _cmd_workflow(args)
        if not args.backend:
            return _fail(args, "invalid_params", "--backend is required without --workflow")
        if not args.worktree and (args.worktree_base is not None or args.worktree_branch is not None or args.worktree_detach):
            return _fail(args, "invalid_params", "--worktree-base, --worktree-branch and --worktree-detach need --worktree")
        if args.goal is not None or args.goal_file is not None:
            return _fail(args, "invalid_params", "--goal is available only with --workflow")
        return _cmd_detached(args, _run_action(args))
    if args.command == "resume":
        return _cmd_detached(args, _resume_action(args))
    if args.command == "continue":
        return _cmd_detached(args, _continue_action(args))
    if args.command == "recovery-activity":
        from .recovery_activity import page
        log_dir = default_log_dir()
        from .workflow_inspection import managed_reader, guard_task_read
        try:
            task_id = store.validate_task_id(args.task_id)
            # Qualify only bounded metadata: this display page never needs the caller's
            # assignment, raw-stream replay, resolved outcome, or full status snapshot.
            from .catalog import METADATA_BYTES, current_metadata_budget
            record = store.read(log_dir, task_id, include_prompt=False,
                                metadata_byte_limit=METADATA_BYTES, metadata_budget=current_metadata_budget())
            try:
                guard_task_read(task_id, managed_reader(log_dir))
            except (StateOwnershipError, store.TaskDocumentError, WorkflowRunDocumentError, WorkflowReceiptDocumentError):
                raise
            except Exception as exc:
                raise _StatusReadRefused("workflow_read_refused", str(exc)) from None
            if record is None:
                raise _StatusReadRefused("unknown_task", f"unknown task_id: {task_id}")
            result = page(log_dir, record, args.cursor, args.limit)
        except _StatusReadRefused as exc:
            return _fail(args, exc.code, exc.message)
        except ValueError as exc:
            return _fail(args, "invalid_params", str(exc))
        print(json.dumps(envelope(CTL_FORMAT, {"v": CTL_JSON_VERSION, "task_id": task_id, "recovery_activity": result})))
        return 0
    if args.command == "status-batch":
        return _cmd_status_batch(args)
    return _cmd_status(args)


if __name__ == "__main__":
    sys.exit(main())
