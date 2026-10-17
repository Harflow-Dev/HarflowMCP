# Waiting for tasks and workflows

Use `wait_for_tasks(task_ids=["task-a", "task-b"], mode="any", timeout_seconds=30)`
to observe up to 32 unique task IDs. Every ID must exist and pass the caller's task-read
permissions before observation starts. Empty, duplicate, oversized and missing IDs are errors.
Non-live IDs receive concurrent filesystem existence checks before any metadata reads or
observers start. A check that times out or fails (for example, permission denied) rejects
the request as unable to validate existence; it is never treated as missing or unavailable.

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

## One wait contract

`wait_for_task`, `wait_for_tasks` and `wait_for_workflow` share one contract. All three are
read-only observers: a wait never writes a record, signals a process or resumes a session, and
timing out or cancelling the request only stops the wait's own temporary observers.

- `timeout_seconds` accepts integers 0 through 3600. Zero performs one observation without
  sleeping. Defaults are unchanged: 55 for `wait_for_task`, 30 for `wait_for_tasks` and
  `wait_for_workflow`. A request is honoured as asked and never silently clamped; out of range is
  `INVALID_PARAMS`. **Breaking change:** `wait_for_task` now accepts 0 and rejects values above
  3600, and the old silent 45 second cap on `wait_for_tasks` and `wait_for_workflow` is gone.
- `until` is `"timeout"` (default) or `"terminal"`. `"terminal"` holds until the stop condition,
  bounded only by a 6 hour server ceiling so an observer cannot be pinned forever. Omit
  `timeout_seconds` with it; passing both is `INVALID_PARAMS`.
- Progress notifications are sent every 5 seconds on all three tools, best effort: a client that
  rejects or ignores them never breaks the wait.

Every response carries the same additive fields: `requested_timeout_seconds` (null for
`until="terminal"`), `effective_timeout_seconds`, `until`, `timed_out`, `stop_reason`,
`elapsed_seconds` and `polling_guidance`. `wait_for_task` keeps its full status payload and
`next_step`. `stop_reason` is one of:

| `stop_reason` | Meaning |
| --- | --- |
| `terminal` | The target settled (or, for `wait_for_tasks`, the mode was satisfied). |
| `attention` | Workflows only: the run reached `needs_input`, `needs_attention` or `paused`. |
| `observation_unavailable` | The target could not be observed for 3 consecutive rounds, or its outcome cannot be observed (see below). |
| `timeout` | `timeout_seconds` elapsed; the target is unaffected. Call again. |
| `ceiling` | The 6 hour `until="terminal"` ceiling elapsed. Call again. |

`timed_out` is true only for `timeout` and `ceiling`.

### Tasks

`wait_for_task` on a task from an earlier server process polls its recorded process and decides
settlement through the same status resolution as `get_task_status`, never the record's own
status. A record that cannot be re-read for 3 consecutive rounds stops an `until="terminal"` wait
with `observation_unavailable`. A recovered task whose process is confirmed gone but whose exit was
never observed stops either mode with `observation_unavailable` and `outcome_observed: false`
(unless its owning polybridge server is still alive, in which case the wait keeps observing for a
15 second grace while that server persists the outcome); every recovered response carries
`outcome_observed` (true otherwise), and a live task carries it (true) once finished. The reported status is exactly what `get_task_status` resolves, an inference rather than an
observed outcome. If the status snapshot itself cannot be built after such a stop, a minimal
`unknown` payload is returned instead of an error. If the task has settled by the time the hold
ends, `stop_reason` is `terminal` and `timed_out` is false.

`wait_for_tasks` additionally lists `undecidable_task_ids`: recovered tasks whose process is
confirmed gone without an observed exit (status `unknown`, `status_source` `recovered_dead`).
They count as settled in both modes, exactly as in `wait_for_task` (one rule: a process confirmed
gone without an observed exit settles the wait with an inferred, unobserved outcome, after the
same owner grace). A process whose identity is merely uncertain is never settled and keeps the wait going. Under
`until="terminal"`, `mode=all` stops with `observation_unavailable` when any task stays
unavailable for 3 rounds, and `mode=any` when every unsettled task does. With `mode=any`,
`terminal` can come back while other tasks are still active; `polling_guidance` says so. Rows carry a bounded
`failure_diagnostic` (`category` and `reason` only) when one exists, such as a usage limit; use
`get_task_status` for the rest.

### Workflows

`wait_for_workflow` returns when the run leaves the active set (`running`, `pending`,
`building`, `pausing`, `starting`, `cancelling`): terminal statuses report `terminal`, and
`needs_input`, `needs_attention` and `paused` report `attention`. A terminal run that is still
`settling` keeps waiting. A status read that fails mid-wait is retried for 3 rounds, then the
last good snapshot is returned with `observation_unavailable`. Under `until="terminal"`, a
terminal run that stays `settling` with its run file unchanged for 300 seconds also stops with
`observation_unavailable` (the run is unaffected; inspect it with `get_workflow_status`).
`polybridge-ctl workflow-wait <run> [--timeout-seconds N | --until terminal]` exposes the same
contract. The initial read still fails
for an unknown run id.

## MCP client request deadlines

Your MCP client applies its own deadline to each tool call, independent of these timeouts.
Exceeding it fails the call while the dispatched run continues unharmed, so a wait is always
safe to repeat with the same arguments. Measured 2026-10-17 with
`scripts/probe-mcp-client-deadlines.py` (a stub stdio MCP server holding a request open, driven
through a localhost fake model API; isolated config homes; no model calls). Other versions and
configurations were not measured.

- **Claude Code 2.1.302** (`claude -p --strict-mcp-config`): sends a `progressToken`. With default
  settings, holds of 70, 130 and 330 seconds returned normally with and without progress every 5
  seconds; no deadline was observed up to 330 seconds, and longer was not measured. With
  `MCP_TOOL_TIMEOUT=30000` the call fails at exactly 30 seconds with or without progress, so
  progress does not extend that deadline, and the server sees the request cancelled.
  If `MCP_TOOL_TIMEOUT` is set, it bounds every wait.
- **Codex codex-cli 0.163.0** (`codex exec`): sends a `progressToken`. The default
  `tool_timeout_sec` deadline is 300 seconds: holds of 70 and 130 seconds completed, and a 330
  second hold with progress failed at exactly 300 seconds ("timed out awaiting tools/call after
  300s"). Progress does not extend it. `tool_timeout_sec = 600` on the server entry let a 330
  second hold complete; `tool_timeout_sec = 30` failed a 70 second hold at 30 seconds, and `90`
  failed a 130 second hold with progress at 90 seconds. Codex did not cancel the request on the
  server side at the deadline: it stayed open until the client exited. An abandoned wait therefore
  keeps observing until its own timeout or the ceiling, which is harmless (waits are read-only) and
  is why `until="terminal"` has a server ceiling.
  Probe configuration notes: Codex reaches MCP tools through code-mode `exec`, which yields
  ("Script running with cell ID N") after about 30 seconds and the model collects the result with
  `wait`; and under `approval_policy="never"` the server entry needed
  `default_tools_approval_mode = "approve"` or the call was refused. Polybridge's installer does
  not set either.

Guidance: on Codex, a wait longer than 300 seconds needs `tool_timeout_sec` raised on the
polybridge server entry; otherwise call the wait again with the same arguments, or keep bounded
waits at or below 270 seconds. On Claude Code, `MCP_TOOL_TIMEOUT` (if set) bounds every wait.
Polybridge still sends progress every 5 seconds because it is cheap and other clients may honour
it, but neither measured client extends its deadline on progress. When unsure of a client's
deadline, keep holds short.
