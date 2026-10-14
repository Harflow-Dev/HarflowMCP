# Fresh task continuation

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
