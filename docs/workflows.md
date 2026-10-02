# Workflows

A workflow is a saved graph of agent steps. An orchestrator agent interprets its conditions and
reports a structured next-step decision. Polybridge validates that decision, launches the steps,
and records the run. Conditions are instructions to an agent, not code executed by Polybridge.

## Build and run

The macOS Monitor lists workflows in the sidebar, with a canvas editor and a run screen. Its Graph view
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
Conditional bypasses may share downstream steps with longer routes. Polybridge checks the
paths actually selected: mutually exclusive alternatives are allowed, while parallel choices
that duplicate work before their shared convergence are refused.
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

## Edit the current canvas with an agent

Choose **Edit with agent** in the editor to refine the currently placed steps, including unsaved
changes. Describe the changes and select a repository for read-only context. The builder can read
repository instructions and available skills. Its activity uses the existing workflow run screen.
Choose **Apply proposal** after completion, then explicitly **Save** to persist the result. **Return
to canvas** keeps the original draft; failed refinement does not replace it. If the source canvas
changed, applying is refused so newer edits remain intact. Historical proposals retain the original
saved baseline and revision, so Save still detects concurrent updates.

CLI callers can pass a current canvas file (it may be incomplete) and optional saved metadata:

```sh
polybridge-ctl workflow-build my-workflow --repo /absolute/path/to/repo \
  --prompt "Add a review step" --backend codex --definition canvas.json \
  --source '{"name":"my-workflow","revision":3,"saved_definition":{}}' --json
```

`workflow_builder` accepts the equivalent optional `definition` object and `source` object.
Refinement returns a run ID; its validated `generated_definition` is an unsaved proposal in the run
record. It never overwrites the stored workflow. Existing generation without `definition` retains
its create-draft behavior.

Builder activity appears as a regular agent conversation beside the canvas. Each accepted
`apply_workflow_draft` update previews the placed steps while the agent works; these revisions
are separate from saved workflow revisions and never write the saved definition. The builder
receives its current draft revision and publishes updates with `expected_draft_revision`.
Only the verified active builder task can publish a preview.

Chat uses the same task composer. Messages join the current turn when supported, or queue for
the next builder turn in the same run and conversation. CLI callers use:

```sh
polybridge-ctl workflow-builder-followup RUN_ID --prompt "Add tests after implementation" --json
```

The builder can publish a preview through MCP `apply_workflow_draft(definition,
expected_draft_revision)` or `workflow-builder-apply --definition draft.json
--expected-draft-revision N --json`. This action derives the run from the verified caller;
it accepts no caller-supplied run or task identity. Final proposals still require explicit Apply
and Save in the editor.

## Limit an inferred retry arrow

Select an arrow returning to an earlier step, enable **Limit retries**, and choose **Max retries**.
The optional connection field `max_retries` counts committed traversals of that arrow across the
entire run; zero disables the retry. Omitting it keeps existing behavior. Node attempt and workflow
limits still apply. A configured limit remains on the connection during topology edits and applies
only when the connection is inferred as a retry.

When a run exhausts an arrow's limit, inspect the result and choose **Continue with one more retry**
to grant one additional traversal to the exhausted path. This explicit continuation preserves your
instructions and does not change the saved workflow's limit. The workflow validator returns its
canonical definition so the inspector uses engine-inferred retry metadata rather than stale flags.

Creating or editing a workflow does not require a repository. Omit `--repo` from
`workflow-build` (or omit `repo_path` from `workflow_builder`) to use Polybridge's
private `~/.polybridge/builder-workspace`. The builder remains read-only, and
followups reuse that workspace. Supply a repository when the builder needs its
instructions or skills; supplied paths still receive the normal repository
validation. Running a workflow still requires a repository.

Workflow task prompt previews and initial chat messages show the user's request.
Polybridge sends role guidance, graph context and result instructions to the
backend separately from that display projection. Raw diagnostic backend logs may
contain the complete execution payload.

### Harness-global MCP approvals

Settings → Harnesses → MCP approvals lists native global approval rules for each harness.
Use `server/tool` for an individual MCP tool or `server/*` for a server-wide rule.
Vibe requires individual tool entries. Adding or removing a rule requires explicit confirmation;
Polybridge does not approve tools automatically after an agent error. The recovery action offers
approval for Polybridge tools and does not restart or resume the agent automatically.

The CLI equivalent is `polybridge-ctl mcp-allowlist --backend codex --json`, with
`--allow polybridge/apply_workflow_draft` or `--remove polybridge/apply_workflow_draft`.
Rules are written to the harness's native global configuration, with a private backup of the
previous file. Harness deny/ask rules, agent profiles and project configuration may still take
precedence. New sessions load updated configuration. Removing an approval restores the harness's
fallback policy rather than restoring a previous explicit per-tool policy.

Start steps may contain an optional `prompt` describing the workflow's purpose.
Polybridge includes that purpose in orchestrator decision context alongside the
request supplied when running the workflow. It complements the runtime request;
task prompt previews and chat continue to show the actual user request.
