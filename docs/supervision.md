# Supervision and recovery

An MCP server observes tasks; a detached task owner executes each task, drains its
output and handles its live input. Closing or restarting the MCP server does not
close the agent's pipes. Another compatible server using the same selected state
root reads the existing task and output instead of starting another execution.
Multiple servers may observe the same task.

The task owner retains the dispatched backend, model, permissions, network
request, session identity, lineage and caller text. Reconnection does not grant
permissions or resume a session. Live messages still return `queued`; the owner's
input pump accounts for delivery through the durable inbox. Approval decisions
remain bound to their original owner generation, process, session and action.

## Launch and settlement

A durable launch reservation precedes process creation. It protects the task ID
and resumed session before ordinary task metadata exists. A private inherited
pipe transfers the prepared invocation to the owner; a launch file alone cannot
authorize execution. Concurrent reconnects never execute the reservation again.
A proven pre-execution failure permits an identical retry. An interrupted or
unacknowledged launch remains reserved when execution cannot be ruled out.

Task observations make at most three attempts to read metadata when its source is positively
observed to change during that read. Each attempt reads the source again with the same
limits and shared metadata budget; existing catalog cache updates may still occur.
Malformed metadata and other failures are not retried. If the bounded reads still fail,
the observation retains uncertainty and refuses the dependent action. This retries only
the metadata read, never execution, message appends, reservations or recovery itself.

Only the task owner's monitor publishes terminal settlement, after observing
process exit and completing bounded drainage and input reconciliation. An
observed exit and an inferred process death are different evidence. Drain failure
can produce terminal history without certifying that all output was captured;
fresh continuation still requires observed exit and complete drainage.

Automatic continuation stays with the detached owner through cutoff settlement,
the continuation reservation and successor acknowledgement. Existing chain
budgets, cancellation markers and permission checks apply. An observer restart
does not spend the budget again or launch a second successor.

## Task-owner failure

The task owner is a separate failure boundary. If it dies, its pipes cannot be
reattached. Harflow preserves captured output, recorded outcomes, launch
reservations and process identities. Status reports lost capture or uncertain
ownership instead of claiming continued supervision or inventing an exit code.
An unresolved execution protects its session and prevents duplicate dispatch.

Cleanup uses verified saved process identities. An undecidable identity is not
permission to signal a process. Cancellation may therefore require attention
when the engine cannot prove which process it would stop. Historical tasks owned
by an earlier in-process server retain their existing conservative recovery
behavior; reconnection does not convert their lost pipes into supervised pipes.

Qualified backend transcript recovery remains display-only. It cannot replace
exit or drain evidence, change outcomes or authorize continuation.

## Workflow scheduler recovery

The workflow scheduler is already detached from MCP clients. Losing an MCP
server does not restart that scheduler. If the scheduler itself dies, recovery
requires an explicit action with a reason:

```sh
harflow-ctl workflow-recover RUN_ID --reason "Inspected the interrupted scheduler" --json
```

The MCP equivalent is `recover_workflow`. Recovery verifies the root scheduler's
death, linked child runs, existing task owners and durable reservations before
resuming scheduling. Unknown ownership blocks recovery. Status and wait calls do
not relaunch schedulers, and child workflows do not independently claim their
root's scheduler ownership.

Recovery reuses issued task IDs, recorded orchestrator attempts, decisions and
child invocation IDs. It reconstructs occupied capacity and checkout protection
before permitting new work. Existing task deadlines and cancellation intent
remain in effect. Completed work, question-answer receipts, selected branches,
parallel join arrivals and goal revisions remain part of the same run.

Native transport recovery requires durable correlated lifecycle observations
and replay through the owner's final observation watermark. Parent exit alone
does not prove child settlement. Missing observations or uncertain native
ownership block retry, fallback and final tree settlement. This does not qualify
or enable a native adapter that is otherwise unavailable.

## Waiting on commands

The live background idle bound applies only after an agent result while no turn
is active and unfinished background work remains. It must not stop an active
foreground command. The default remains 600 seconds for background-only silence;
see [task continuation](task-continuation.md#live-background-wait-bound) for
configuration and diagnostics. MCP wait timeouts bound observation, not task
execution.

## Companion application

The application observes the existing public ctl snapshots and event streams.
Its immutable managed engine must be updated deliberately after an engine
release; changing this checkout does not change the application's paired engine.
Private launch and native-observation documents are engine-owned state, not
files the application reads or writes.

## Dead executions and disk capacity

A crash can leave a claimed or spawned launch reservation without a terminal receipt. Resume
rechecks the saved owner and agent identities. A matching launch/task owner and session, with both
processes confirmed dead, no longer holds the conversation busy. This also applies to task handles
already attached in the observing server. The original reservation is retained; this is a new
resume using the recorded session ID, not replay of the original execution and not an observed
success or exit code. Live or indeterminate identities remain busy. Missing records, mismatched
owner/session identity, and ambiguous pre-execution reservations remain reserved. Restore process
inspection access, wait for a live run or close an active takeover before retrying; never delete
reservation, lock or store files to bypass refusal. Context is recoverable only if the backend
persisted a resumable session and its history remains available.

Before creating a launch reservation, and again before executing in the owner, Harflow checks for
at least **256 MiB (268435456 bytes)** free on each distinct filesystem used for task state/output,
the checkout and temporary storage. This fixed reserve leaves room for metadata, logs and agent
startup; it is a preflight, not reserved capacity or an estimate of the assignment's disk needs.
Below the threshold, an actionable `Disk nearly full` refusal reports available/required bytes and
the filesystem path. Failed capacity discovery also refuses before agent creation. Free space and
retry. A failure after a reservation but before execution marks a proven not-started attempt for
safe retry when that write succeeds; otherwise uncertainty remains reserved.

The disk can still fill after preflight. Raw-log write or buffered-close failures disable raw
capture while stdout continues draining; status notices disclose the lost capture. Metadata writes
retain the last successfully replaced record. No exit receipt or task success is fabricated if it
cannot be persisted. Recovery may show uncertain status or incomplete output until disk access is
restored; in-memory output and files whose writes never succeeded cannot be reconstructed.

## Dispatch origin, owner and responding server

`owner` identifies the process that executes the agent and captures output; it is normally a
separate detached process, so its PID can differ from the MCP server immediately after startup.
Optional persisted `dispatch_origin` identifies the server that requested that execution.
`responding_server` identifies the process serving the current observation. Display projections
include only PID and process start time for the latter two identities. These are diagnostic roles,
not caller authentication or additional execution authority.

`observation_mode` and `observation_note` explain their relationship:

- `owner`: the responding process is the execution owner.
- `attached`: the responding process matches the recorded dispatch origin; this is normal detached
  operation, including a task that finishes before its first status request.
- `recovered`: a different responding process is reading a task with a known origin. This does not
  establish that either process died or that execution ownership changed.
- `unknown`: origin or responder process-generation evidence is missing. Old records remain valid;
  Harflow does not invent an origin or infer a restart.

Matching requires PID and nonempty process start time, so PID reuse does not establish continuity.
`recovered` is false for owner/attached, true for recovered/unknown for conservative compatibility.
Use `observation_mode` rather than treating that boolean as proof of a crash. Actual owner/process
liveness and capture continuity remain separate `supervision` evidence. Responding identity is
recomputed for snapshot, brief, compact and indexed-list reads; cached headers do not establish
which server is responding now. A size-pruned origin yields unknown rather than a continuity claim.

Detail and compact status reads add `supervision.dispatch_origin_state` when a dispatch origin is
recorded: `alive`, `dead`, or `undecidable`, using process-generation-qualified evidence. Matching
the current responding process proves that origin alive without an extra probe. Missing origin
omits the diagnostic; missing generation stays undecidable. A different observer with a live
origin and a different observer with a dead origin are therefore distinguishable while keeping
existing observation modes. Dead dispatch origin does not establish replacement: a short-lived
ctl dispatcher can exit normally. This diagnostic never changes execution ownership, capture or
session-busy decisions, and indexed listing does not add per-row dispatcher probes. Observed-terminal history also
skips external dispatcher probes in briefs, lists and batch status; it reports undecidable unless
the origin is the current responding process, whose liveness is already established.

Observed settled live-input tasks refuse sends as `settled`, naming their terminal status, before
checking sealed input. A running sealed task still returns `closed` and its closure reason. A
terminal-looking record without an observed outcome retains closed/exited/uncertain guidance;
its saved status is not proof that the process settled.


A confirmed dispatcher exit with a live execution owner is a normal observation change.
Full task snapshots expose stable optional `supervision.adopted_after_restart` metadata;
compact MCP polling shows it once per reader generation and keeps the obsolete origin in
full diagnostics. The timestamp identifies the new reader's process start, not the exact
crash. Owner/capture/input uncertainty remains visible and does not qualify as safe adoption.
See [the projection contract](contracts.md#surviving-execution-after-dispatcher-restart).
The Mac restart-note presentation remains pending separate design/implementation.
