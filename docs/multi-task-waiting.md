# Waiting for multiple tasks

Use `wait_for_tasks(task_ids=["task-a", "task-b"], mode="any", timeout_seconds=30)`
to observe up to 32 unique task IDs. Every ID must exist and pass the caller's task-read
permissions before observation starts. Empty, duplicate, oversized and missing IDs are errors.

`any` returns as soon as at least one requested task is terminal. `all` returns when every
requested task is terminal. Responses include `tasks` in input order, `terminal_task_ids`
in input order, bounded identity/title/summary fields and a `get_task_status` diagnostic
reference. They contain no output tails and remain below 64 KiB serialized JSON. Recovered
records expose `recovered`, `status_source` and a bounded resolution `note` so inferred
liveness outcomes remain distinguishable from observed exits. Disappearing or unreadable records report `unavailable` without changing tasks.
Recovered records use bounded metadata and process identity; their persisted status alone is never authoritative.
Recovered summaries are absent: waiting never reads prompt sidecars or stream logs. Metadata
reads are capped at 256 KiB each with a shared two-second I/O budget per observation round.
Unobserved dead or uncertain outcomes report `unknown`, never terminal proof. Live terminal
status only counts after the task completion event confirms output settlement.

Timeout accepts integer values from 0 through 300 seconds, defaults to 30, and holds the
observation delay to at most 45 seconds. Bounded metadata I/O can add up to two seconds
for initial validation and two seconds for a final observation round; reported effective
timeout describes the observation delay. Zero performs one observation without sleeping. Inspect
`requested_timeout_seconds`, `effective_timeout_seconds`, and `timed_out`; poll again while
tasks remain active. Progress notifications are best effort. Timing out or cancelling the
request only stops temporary observers; it never signals, cancels or changes a task.

`wait_for_task` retains its existing API and output. Existing `wait_for_workflow` already
accepts 0..300 seconds, defaults to 30, caps its effective hold at 45 seconds, and returns
requested/effective timeout and polling guidance. It waits for workflow attention/completion;
use it for workflow state rather than reconstructing workflow progress from task waits.
