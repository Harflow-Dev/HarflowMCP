# Fresh task continuation

[Extra writable paths](extra-writable-paths.md) carry the source's saved typed grants,
certification and identities into the fresh session. They have no continuation override.
Changed identities, unsupported versions and incompatible recovery reject reuse.

`continue_task(task_id, extra_prompt=None, notes_path=None)` launches a new agent session from an existing assignment. It carries the original request, latest summary, observed outcome and optional caller notes into a fresh context. Caller prose is preserved. Evidence exceeding 64 KiB is referenced by its local file path; missing original context is refused. Notes must be a regular UTF-8 file; large notes are referenced without reading them completely.

The source must have a durably recorded terminal status, an observed process exit, fully drained output and a finished input pump. Old records without drain evidence and recovered runs without observed exit are ineligible. Workflow-owned tasks and managed workflow callers must use their workflow supervisor instead.

The new session inherits backend, model, reasoning effort, freedom, network request, turn cap, group and title. `continuation_of_task_id` records the predecessor and `chain_root_task_id` identifies the original assignment. These fields are independent of same-session resume ancestry and delegation ownership. Continuation does not expand permissions.

`start_task(..., auto_continue=N)` accepts integers from 0 through 10, default 0. The budget counts fresh successor sessions. Each automatic successor decrements the remaining budget. Manual continuation starts with automatic continuation disabled.

Automatic continuation currently recognizes Claude's structured `error_max_turns` result with observed exit 0 and Vibe's exact live-turn `Turn limit of N reached` stop envelope with observed exit 1. Signal exits, unrelated error evidence and unfinished later work refuse automatic continuation. Other backends have no certified cutoff classification and reject a positive automatic budget. `supports_auto_continue` advertises this distinction. Cancellation, permission denial, provider usage limits, unobserved termination, generic failures and absence of a final message do not trigger continuation.

A cross-process chain lock and durable predecessor reservation prevent duplicate successors. A reservation with an existing successor is not executed again. An identical retry returns a locally held successor; a changed follow-up or notes is rejected rather than silently dropped. An interrupted or ambiguous spawn remains reserved and fails closed. A proven pre-exec failure can retry the same reserved identifier. Manual continuation can also recover a failed automatic pre-exec attempt with the identical reconstructed context, provided no successor exists; its automatic budget is disabled. Other changes of automatic mode are rejected. Automatic continuation does not survive MCP server death; explicit continuation remains available after eligible durable settlement.

The CLI supports `polybridge-ctl continue TASK_ID --extra-prompt-file followup.txt --notes-path notes.txt`.
For a standalone launch, `polybridge-ctl run ... --auto-continue N` keeps its detached owner alive
until all eligible successors settle. Identical continuation retries share the reserved successor;
different follow-up text or notes are rejected instead of silently dropped. `list_backends` reports
`capabilities.supports_auto_continue`; unsupported automatic settings are rejected before spawn.

Cancelling the initial task also reaches active successors through continuation edges. Cancellation serializes with an in-flight reservation and persists a stopped-chain marker before returning; pending or later successors cannot restart that chain. Start a new task explicitly when you want to begin work again after cancelling an automatic chain. Detached CLI owners remain alive until all queued continuation launches and their subprocesses settle.

Retention preserves the original chain assignment while any successor record remains, including settled successors still within retention and records whose cleanup is locked or failed. Fully expired chains are reclaimed once their successor records have been removed.

## Queued follow-ups

A task that cannot take a message while it runs (a backend without live input, or claude started
with `max_turns`) can still be told something for later. `send_message(task_id, text)` on such a
standalone task, running or settled, appends the text to a durable per-task ledger and answers
`{"status": "queued", "delivery": "next_continuation", "message_id", "seq"}`. It never says
"delivered": nothing is started and nothing is written to an agent. Live-input tasks keep their
inbox path unchanged and answer `delivery: "live_input"`; a live task whose input has closed is
still told to `resume_task`, not redirected here. The ledger is separate from the live-input inbox
and never touches its lock or close protocol. `polybridge-ctl send` takes the same route.

**Consumption.** The next continuation a caller starts explicitly from the task consumes every
queued message, in `seq` order, exactly once: `resume_task`, `continue_task`, `polybridge-ctl
resume` and `polybridge-ctl continue` (the Monitor's Resume goes through ctl). Automatic
continuations, workflow supervisor resumes and builder resumes never consume. A resume's prompt is

```
Queued caller follow-up 1 of N (message <id>, queued <iso time>):
<text exactly as sent>

...one block per message, separated by a blank line, then a blank line, then:
<followup_prompt verbatim>
```

or, when `followup_prompt` is omitted, the fixed last line `End of queued caller follow-ups.`.
`followup_prompt` (and the `polybridge-ctl resume` text) may be omitted only when something is
queued; blank strings are still rejected. `continue_task` places the same blocks ahead of its
`Caller follow-up:` section (or the closing line when there is none). With nothing queued every
call behaves exactly as before. The successor's `task_started` event carries `message_ids`.

**States.** `queued`, `reserved` (handed to a successor that is being started), `consumed`,
`cancelled`, `undeliverable` (a successor already exists: resend to it) and `delivery_unknown` (a
launch failed after the messages were handed over, so they may have reached an agent; inspect, do
not blindly resend). Consumed and cancelled items keep metadata only (newest 20; `omitted_final`
counts the rest). `get_task_status` and `polybridge-ctl status --json` list them as
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

An unreadable ledger (damaged, over its read budget, or not a valid ledger) reads as `unavailable`.
It blocks consuming `resume_task` and `continue_task` for that task, refuses sends, and keeps the
task from retention, because it cannot be shown to be empty. The error names the file
(`<task_id>.followups.json` in the task log directory): inspect it, then repair or delete it.
`queued_followup_count` is on briefs and full snapshots, but not on the cached compact-listing
headers (`list_tasks` pages, `task-list-page`). Status reads never write the ledger; a resolution
they observe is persisted by the next send, cancel, consuming continuation or retention pass.

Retention keeps a task while it has queued or reserved messages, and the successor a reservation
names; `undeliverable` and `delivery_unknown` items do not block it. Cancelling a task leaves its
queued messages in place: a cancelled task can still be resumed.
