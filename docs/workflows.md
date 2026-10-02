# Workflows

A workflow is a saved graph of agent steps. An orchestrator agent interprets its conditions and
reports a structured next-step decision. Polybridge validates that decision, launches the steps,
and records the run. Conditions are instructions to an agent, not code executed by Polybridge.

## Build and run

The macOS Monitor provides a Workflows library, a canvas editor, and a run screen. Its Graph view
shows the canvas above the same agent activity columns used by Parallel. Parallel view expands
those columns. Highlighting comes from recorded task state. Agent names and colored dots identify
backends; vendor logos are unnecessary.

The menu bar also shows workflow run status alongside agent activity. Its popover lists workflows
by name and status, including runs that need attention; selecting one opens that workflow run.

The canvas supports Start, Agent, and End nodes. Connect parallel branches to the same next node;
it waits for all active branches automatically. Agent steps have four roles:

| Role | Purpose | Default access | Allowed access |
|---|---|---|---|
| Planning | Produce a task list for the run. | `read_only` | All four levels |
| Implementation | Implement work and report evidence. | `write_in_repo` | `write_in_repo`, `publish`, `unrestricted` |
| Review | Examine results and report issues or approval. | `read_only` | All four levels |
| Task | Perform a specific action, such as updating Jira. | `publish` | All four levels |

Planning creates pending checklist entries. Workers can report progress, but **only validated
orchestrator decisions complete or reopen checklist items**. The Monitor shows that status without
a manual completion toggle.

Access levels are `read_only`, `write_in_repo`, `publish`, and `unrestricted`.
The launch access is a ceiling: each worker uses the lower of its configured access and that
ceiling, and fallback agents use the same effective access. The default launch ceiling is
`write_in_repo`, so a Task configured for `publish` is capped until the caller raises it.
A read-only launch containing an Implementation step is rejected. Actual access is recorded
on each dispatch. Network access is a separate optional boolean (`--network true|false`);
selecting Publish or Unrestricted does not independently request network access. Backend
capability restrictions still apply.

For example, a Task step that updates a Jira issue can request `"freedom": "publish"`.
Launch that workflow with `--freedom publish` to retain that access; launching with
`--freedom write_in_repo` caps the step. Set network separately where the backend supports it.

Each agent step configures its instructions, primary candidate, fallback candidates, access,
session mode, and attempt budget. The orchestrator has its own candidate list, saved with the
workflow and overrideable when launching. Workflow generation has a separate candidate list.

MCP tools expose definition list/get/save/delete, `workflow_builder`, run list/start/status/wait,
and pause/resume/cancel. The Monitor acts through matching `polybridge-ctl workflow-*` commands;
it never writes Polybridge state directly. Definition saves use an expected revision to reject
concurrent edits. Editing or deleting a definition does not change existing run snapshots.

For example, load the supplied [implementation/review workflow](../examples/workflows/implement-review.json):

```bash
polybridge-ctl workflow-save implement-review \
  --definition examples/workflows/implement-review.json --expected-revision 0 --json
polybridge-ctl workflow-start implement-review --repo /absolute/path/to/repo \
  --prompt "Implement the requested change" --freedom write_in_repo --json
polybridge-ctl workflow-status <workflow_run_id> --json
polybridge-ctl workflow-pause <workflow_run_id> --json
polybridge-ctl workflow-resume <workflow_run_id> \
  --instructions "Address the reported failure" --additional-attempts 1 --json
```

Validate an editable graph before saving with
`polybridge-ctl workflow-validate --definition workflow.json --json`.
It returns `result.valid` and an optional `result.error` without saving definitions or run state.
Every node must be reachable from Start and have a forward path to End, including loops.

`workflow-list` and `workflow-list-runs` list definitions and runs. `workflow-get` reads a definition;
`workflow-delete` removes one. To generate a draft:

```bash
polybridge-ctl workflow-build my-workflow --repo /absolute/path/to/repo \
  --prompt "Plan, implement, review, then run tests" --backend codex \
  --fallbacks '[{"backend":"claude","model":"opus","reasoning_effort":"high"}]' --json
```

Definition JSON uses `nodes` and `connections`. An agent node's `role` is `planning`,
`implementation`, `review`, or `task`; its `agent.fallbacks` is an ordered array. `session_mode`
is `resume` or `fresh`. Connections specify `source`, `target`, `condition`, and an optional
`default` fallback. Polybridge derives routing metadata (`branch_mode: auto`, `join_id`, and
`backward`) from the graph; callers do not configure it. Legacy routing fields are accepted
and normalized for new runs; existing run snapshots retain their original behavior. Legacy
`join` nodes remain readable. Nodes store canvas coordinates in `position: {x, y}`.

Each role receives a short built-in instruction alongside the task, custom step instructions,
checklist, and prior results. Planning creates pending tasks, implementation reports task IDs
and evidence, review gives a verdict and concrete findings, and task steps report observed
results. Only the orchestrator changes checklist statuses. Select Start to edit workflow
settings, including the orchestrator, fallback agents, and limits.

## Branches and attempts

Outgoing connections carry condition prompts. The orchestrator evaluates evidence and chooses
one or more legal connections. A default connection is available when the conditions are unclear;
the orchestrator can also request attention rather than guess. When parallel
branches share a next node, that node waits for the selected branches of its own
activation and then executes once. Conditional branches that were not selected do not block it.
Nested splits may converge at the same node. Paths without a common forward convergence and
loops crossing an open parallel region are rejected. A retry arrow may share a step with
forward arrows, but selecting a retry is exclusive: it cannot also launch a forward path.

Polybridge infers a retry when an arrow returns to a step that every path to its source passes
through. Canvas placement does not determine direction; ambiguous cycles are rejected.
Retry connections repeat a step. Workflow candidates default to a 100-turn cap where supported (Claude and Vibe).
Explicit limits are preserved; backends without turn-cap support, including Codex, do not
receive an unsupported cap.

Three attempts means the initial activation plus two repeats,
not three additional retries. Defaults are three activations per agent node, 100 transitions per
run, and four concurrently active node tasks. Read-only workflow steps share a checkout lease;
writing workflow steps require exclusive access. These leases coordinate workflow runs, not
manual edits or unrelated tools.

**Resume** retains the same candidate's usable conversation between activations. **Fresh** starts
a new session and supplies previous results as context. A different fallback candidate always
starts fresh. A missing or busy resume session requires attention rather than silently discarding
the chosen session policy.

## Fallbacks

Fallback candidates are ordered and may include different backends or different models on the
same backend. Polybridge advances only after a confirmed availability failure: missing binary,
unavailable model, or a recognized quota/rate-limit rejection. Unsupported configuration is a
validation error. Failed tests, generic crashes, timeouts, or an agent saying it is unavailable do
not automatically authorize a switch.

Each candidate is tried at most once within an activation. Switching candidate does not consume
an extra graph activation. The last working candidate remains selected for later activations;
unavailable candidates remain suppressed until explicitly retried. Every attempted candidate and
the switch reason are recorded.

Existing edits remain in place. Fallback waits for the previous process to settle and receives
known prior results. Ambiguous execution or side effects require attention. Exhausting the
candidate list also requires attention.

## Control and recovery

The detached supervisor continues when the Monitor closes or an MCP client disconnects. Pause
stops scheduling and lets active steps finish. Cancel stops the workflow's associated tasks and
their descendants. Taking over a managed task pauses workflow scheduling before handing the
session to Terminal; manual continuation is not silently adopted as the next workflow step.

At an attempt limit, the run stops scheduling, settles active siblings, and shows **Needs
attention**. Resume can include additional instructions and explicitly granted attempt budget.
Those grants belong to that run, not the saved definition.

Task associations are reserved before launch. Supervisor-crash recovery reconciles existing
records and does not automatically replay uncertain dispatches. Active and Needs attention runs
protect their referenced task records from ordinary retention. Finished run history retains compact
outcomes; expired activity logs are shown as unavailable.

## Storage and permissions

Definitions live under `~/.polybridge/workflows/`. Immutable run snapshots, task associations,
decisions, and control history live under `~/.polybridge/workflow-runs/`. Ordinary agent streams
remain under `~/.polybridge/tasks/` and use the existing event format.

The orchestrator recommends decisions; it cannot grant additional node permissions. Builder and
orchestrator agents run read-only, and workers retain the run's permission limits. Existing backend
enforcement caveats still apply: a workflow does not create an OS sandbox for a backend that lacks
one. Inspect each task's enforcement report.
