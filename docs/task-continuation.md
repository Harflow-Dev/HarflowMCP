# Fresh task continuation

[Extra writable paths](extra-writable-paths.md) carry the source's saved typed grants,
certification and identities into the fresh session. They have no continuation override.
Changed identities, unsupported versions and incompatible recovery reject reuse.

`continue_task(task_id, extra_prompt=None, notes_path=None)` launches a new agent session from an existing assignment. It carries the original request, latest summary, observed outcome and optional caller notes into a fresh context. Caller prose is preserved. Evidence exceeding 64 KiB is referenced by its local file path; missing original context is refused. Notes must be a regular UTF-8 file; large notes are referenced without reading them completely.

The source must have a durably recorded terminal status, an observed process exit, fully drained output and a finished input pump. Old records without drain evidence and recovered runs without observed exit are ineligible. Workflow-owned tasks and managed workflow callers must use their workflow supervisor instead.

The new session inherits backend, model, reasoning effort, freedom, network request, turn cap, group and title. `continuation_of_task_id` records the predecessor and `chain_root_task_id` identifies the original assignment. These fields are independent of same-session resume ancestry and delegation ownership. Continuation does not expand permissions.

`start_task(..., auto_continue=N)` accepts integers from 0 through 10, default 0. The budget counts fresh successor sessions. Each automatic successor decrements the remaining budget. Manual continuation starts with automatic continuation disabled.

Automatic continuation currently recognizes Claude's structured `error_max_turns` result with observed exit 0 and Vibe's exact live-turn `Turn limit of N reached` stop envelope with observed exit 1. Signal exits, unrelated error evidence and unfinished later work refuse automatic continuation. Other backends have no certified cutoff classification and reject a positive automatic budget. `supports_auto_continue` advertises this distinction. Cancellation, permission denial, provider usage limits, unobserved termination, generic failures and absence of a final message do not trigger continuation.

A cross-process chain lock and durable predecessor reservation prevent duplicate successors. A reservation with an existing successor is not executed again. An identical retry returns a locally held successor; a changed follow-up or notes is rejected rather than silently dropped. An interrupted or ambiguous spawn remains reserved and fails closed. A proven pre-exec failure can retry the same reserved identifier. Manual continuation can also recover a failed automatic pre-exec attempt with the identical reconstructed context, provided no successor exists; its automatic budget is disabled. Other changes of automatic mode are rejected. Automatic continuation survives MCP server death because its detached owner retains scheduling responsibility through successor acknowledgement. Task-owner death remains a separate conservative failure boundary: unresolved launch or scheduling evidence stays reserved, and explicit continuation still requires eligible durable settlement. See [supervision and recovery](supervision.md).

The CLI supports `harflow-ctl continue TASK_ID --extra-prompt-file followup.txt --notes-path notes.txt`.
For a standalone launch, `harflow-ctl run ... --auto-continue N` keeps its detached owner alive
until all eligible successors settle. Identical continuation retries share the reserved successor;
different follow-up text or notes are rejected instead of silently dropped. `list_backends` reports
`capabilities.supports_auto_continue`; unsupported automatic settings are rejected before spawn.

Cancelling the initial task also reaches active successors through continuation edges. Cancellation serializes with an in-flight reservation and persists a stopped-chain marker before returning; pending or later successors cannot restart that chain. Start a new task explicitly when you want to begin work again after cancelling an automatic chain. Detached CLI owners remain alive until all queued continuation launches and their subprocesses settle.

Retention preserves the original chain assignment while any successor record remains, including settled successors still within retention and records whose cleanup is locked or failed. Fully expired chains are reclaimed once their successor records have been removed.

## Recovery after a permission denial

When a finished Claude task reports `permission_denials`, read each entry's `denial_category` and
`denial_remedy`. Recovery is always a new `resume_task` or `continue_task` execution of a finished task; a denial or expiry is
never answered later. What to change follows the entry's `denial_remedy`:

- `approval_required` and `chained_command` (Bash): pass `allowed_commands` covering the command, or each part
  of the chain. Omitting the list inherits the saved grants; an explicit list replaces them. Grants are
  validated like `start_task` (grammar, qualified Claude Code version, nested-subset rule).
- A denied or expired Read, Write or Edit approval: resume the run (`caller_approvals` and
  `caller_write_approvals` are inherited) so the agent can raise a fresh request for you to answer.
  `allowed_commands` does not cover file tools.
- `path_safety` and `outside_working_directory`: these go through caller approvals, not grants.
- `deny_rule`: grants cannot lift a deny rule; change the access level or the settings that denied it.

`resume_task` continues the same session and `continue_task` starts a fresh one; both require the source to be
finished and are [documented with the grants](harness-permissions.md#dispatch-time-command-grants-allowed_commands).
`respond_approval_request` applies only to a request that is still pending. Expiry of an unanswered request is a
Harflow rejection and is recovered the same way. This is for standalone tasks; workflow nodes are different (see
[workflows](workflows.md#caller-approval-interaction)).

## Queued follow-ups

A task that cannot take a message while it runs (a backend without live input, or claude started
with `max_turns`) can still be told something for later. `send_message(task_id, text)` on such a
standalone task, running or settled, appends the text to a durable per-task ledger and answers
`{"status": "queued", "delivery": "next_continuation", "message_id", "seq"}`. It never says
"delivered": nothing is started and nothing is written to an agent. Live-input tasks keep their
inbox path unchanged and answer `delivery: "live_input"`; a live task whose input has closed is
still told to `resume_task`, not redirected here. The ledger is separate from the live-input inbox
and never touches its lock or close protocol. `harflow-ctl send` takes the same route.
Persisted live-input rows require the private [inbox row contract](contracts.md#live-input-inbox-rows);
older flat development rows are skipped without conversion. Public queued responses and caller
message payloads remain unchanged.

**Consumption.** The next continuation a caller starts explicitly from the task consumes every
queued message, in `seq` order, exactly once: `resume_task`, `continue_task`, `harflow-ctl
resume` and `harflow-ctl continue` (the Monitor's Resume goes through ctl). Automatic
continuations, workflow supervisor resumes and builder resumes never consume. A resume's prompt is

```
Queued caller follow-up 1 of N (message <id>, queued <iso time>):
<text exactly as sent>

...one block per message, separated by a blank line, then a blank line, then:
<followup_prompt verbatim>
```

or, when `followup_prompt` is omitted, the fixed last line `End of queued caller follow-ups.`.
`followup_prompt` (and the `harflow-ctl resume` text) may be omitted only when something is
queued; blank strings are still rejected. `continue_task` places the same blocks ahead of its
`Caller follow-up:` section (or the closing line when there is none). With nothing queued every
call behaves exactly as before. The successor's `task_started` event carries `message_ids`.

**States.** `queued`, `reserved` (handed to a successor that is being started), `consumed`,
`cancelled`, `undeliverable` (a successor already exists: resend to it) and `delivery_unknown` (a
launch failed after the messages were handed over, so they may have reached an agent; inspect, do
not blindly resend). Consumed and cancelled items keep metadata only (newest 20; `omitted_final`
counts the rest). `get_task_status` and `harflow-ctl status --json` list them as
`queued_followups` (`{state, items, omitted_final}`; `state: "unavailable"` if the ledger cannot be
read), and briefs carry `queued_followup_count`.

**Sealing.** Reserving seals the ledger so a message can never land on a source that already has a
successor: further sends are refused with `superseded` and name the successor. A launch that
provably did not start releases the messages and unseals; one that started consumes them; an
ambiguous one (for example a cancellation inside the process spawn) leaves them `delivery_unknown`
and sealed. A terminal write that fails never changes the launch outcome: the server retries it the
next time it takes the task's follow-up lock, and a status read meanwhile reports the truthful
state.

**Limits.** A message is at most 32 KiB of UTF-8; a task holds at most 16 outstanding messages,
and sends are refused (`queue_full`) once the serialized ledger would pass 960 KiB. Other refusals
are `message_too_large`, `unavailable`, an unknown task and workflow-owned tasks or managed
workflow callers. `cancel_queued_message(task_id, message_id)` cancels a message that is still
`queued` under the same checks as a send.

**Manual `continue_task` retries.** The queue is read once, when the successor is reserved. Two
manual calls with the same `extra_prompt` and notes are the same request whatever is queued and
share one successor; a retry returns or names that successor (from another server process it fails with `Continuation already exists: <id>`) without recomposing consumed prose. After a
proven pre-exec failure a retry reserves the then-current queue.

The ledger uses the qualified `harflow.task.followups` envelope described in [contracts](contracts.md#queued-follow-up-ledgers). Observable source replacement can conservatively report unavailable until a later operation reads a stable qualified source.

An unreadable or unqualified ledger (damaged, over its read budget, foreign, unversioned, or incompatible) reads as `unavailable`.
It blocks consuming `resume_task` and `continue_task` for that task, refuses sends, and keeps the
task from retention, because it cannot be shown to be empty. The file is `<task_id>.followups.json` in the task log directory. Preserve it for manual inspection outside the engine; readers never repair, relabel or adopt it. For recovery, preserve the old state root and select a separate fresh missing or empty root. No historical conversion is supported.
`queued_followup_count` is on briefs and full snapshots, but not on the cached compact-listing
headers (`list_tasks` pages, `task-list-page`). Status reads never write the ledger; a resolution
they observe is persisted by the next send, cancel, consuming continuation or retention pass.

Retention keeps a task while it has queued or reserved messages, and the successor a reservation
names; `undeliverable` and `delivery_unknown` items do not block it. Cancelling a task leaves its
queued messages in place: a cancelled task can still be resumed.

## Live background wait bound

Standalone start, resume and continue accept `background_idle_seconds`;
`harflow-ctl run|resume|continue` accepts `--background-idle-seconds`. The value
must be finite and greater than zero, at most 21600 seconds; booleans are
rejected. Omission inherits the effective saved bound for continuation. A fresh
dispatch resolves `HARFLOW_LIVE_IDLE_SECONDS` once, falling back to 600 seconds
for invalid environment values, and persists the result. Explicit continuation
overrides participate in request idempotency. Caller assignment text remains
verbatim; qualified Claude live input receives a separate runtime context block
explaining its effective bound. `HARFLOW_BACKGROUND_IDLE_SECONDS` also reports
it in the child environment.

Only a backend advertising `tracks_background_tasks` applies background idle
waiting. Classic/capped runs do not use live-input idle waiting. At the bound,
open background work fails the run even if the latest agent result succeeded;
status exposes the detailed failure diagnostic instead of implying completion.
