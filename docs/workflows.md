# Workflows

A workflow is a saved graph of independent agent steps. The orchestrator owns the original request,
workflow context, and checklist. At each stage it selects a valid continuation and writes a focused
assignment for the next agent. Polybridge validates, dispatches, and durably records the decision.
Conditions are instructions to the orchestrator, not code executed by Polybridge.

## How the runner works

Polybridge runs the control loop. The orchestrator decides what to do; worker harnesses execute
focused assignments and return structured results. The arrows below represent harness prompts
and final JSON messages—not MCP calls between agents.

```mermaid
sequenceDiagram
    participant Caller as Caller (MCP tool or Monitor)
    participant Runner as Polybridge runner
    participant Orchestrator as Orchestrator harness
    participant Worker as Worker harnesses

    Caller->>Runner: Start workflow with original request
    Runner->>Runner: Snapshot workflow and persist run
    loop Each decision checkpoint
        Runner->>Orchestrator: Request, workflow context, plan, checklist, results, valid choices
        Orchestrator-->>Runner: Final JSON decision
        alt Invalid decision
            Runner->>Runner: Dispatch nothing and count decision attempt
            Runner->>Orchestrator: Correction and valid choices
            Note over Runner,Orchestrator: Exhausted decision attempts pause for attention
        else Inspect a settled execution
            Runner->>Orchestrator: Stored result or activity page
            Note over Runner,Orchestrator: Stay at the same decision checkpoint
        else Continue with valid assignments
            Runner->>Runner: Validate and persist decision and dispatch reservations
            Runner->>Worker: Assignment, node instructions, selected results and task descriptors
            Note over Runner,Worker: Parallel start launches every branch
            Worker-->>Runner: Final JSON: succeeded, failed, blocked, or asking
            Runner->>Runner: Persist output and execution state
            opt Worker asks for context
                Runner->>Orchestrator: Question and current execution context
                Orchestrator-->>Runner: Answer decision
                Runner->>Worker: Answer and continue the same node execution
                Worker-->>Runner: Final JSON result or another question
                Note over Runner,Orchestrator: Parallel questions are handled one at a time
            end
            Note over Runner,Worker: Matching Parallel end waits for every branch to resolve
        else Needs caller input
            Runner->>Runner: Suspend scheduling while running siblings settle
            Runner-->>Caller: Question and input decision ID
            Caller->>Runner: Resume with answer and matching decision ID
            Runner->>Orchestrator: Answer at the suspended checkpoint
        else Complete or fail
            Runner->>Runner: Validate stopping action and persist outcome
            Runner-->>Caller: Workflow status and available results
            Note over Caller,Runner: Completion requires valid End traversal and settled branches
            Note over Caller,Runner: Failure may still have settling siblings
        end
    end
```

Only the orchestrator owns the original request and global checklist. Planning can return two
separate outputs: task descriptors and a technical plan. Each planning node independently controls
whether either output is required. Polybridge stores supplied outputs, shows them in the Monitor,
and includes them in later orchestrator context. A worker receives only its assignment,
local guidance, and selected inputs. Harnesses can still use their own tools during execution.

The diagram shows the usual loop; fallbacks, retry budgets, optional branches, and failed-run
recovery follow the rules below. External callers answer their own input requests; Monitor-started
runs expose those questions to the user in the app.

Workflow names can include spaces, such as `Feature Implementation`. Names are stored exactly;
quote names with spaces when using the CLI. Names must start with a letter or digit and contain
1–100 letters, digits, spaces, dots, dashes, or underscores, without trailing spaces. Graph and
run identifiers retain their stricter format.

## Build and run

The macOS Monitor lists workflows in the sidebar, with a canvas editor and a run screen. Its Graph view
shows the canvas above the same agent activity columns used by Parallel. Parallel view expands
those columns. Highlighting comes from recorded task state. Agent names and colored dots identify
backends; vendor logos are unnecessary.

The menu bar also shows workflow run status alongside agent activity. Its popover lists workflows
by name and status, including runs that need attention; selecting one opens that workflow run.

The canvas supports Start, Agent, End, Parallel start, and Parallel end nodes. Parallel boundaries
are structural steps: they have no harness, worker assignment, access setting, or agent attempt. Agent steps have four roles:

| Role | Purpose | Default access | Allowed access |
|---|---|---|---|
| Planning | Produce a task list for the run. | `read_only` | All four levels |
| Implementation | Implement work and report evidence. | `write_in_repo` | `write_in_repo`, `publish`, `unrestricted` |
| Review | Examine results and report issues or approval. | `read_only` | All four levels |
| Task | Perform a specific action, such as updating Jira. | `publish` | All four levels |

Planning defaults to requiring a nonempty `tasks` checklist and a nonempty Markdown
`technical_plan`. **Require checklist** (`require_tasks`) and **Require technical plan**
(`require_technical_plan`) are independent booleans; each defaults to true. A scoping brief may
disable either requirement. The Monitor right sidebar shows the technical plan alongside the live checklist.
Planning creates pending checklist entries. Workers can report progress, but **only validated
orchestrator decisions complete or reopen checklist items**. The Monitor shows that status without
a manual completion toggle.

Access levels are `read_only`, `write_in_repo`, `publish`, and `unrestricted`.
Saved node access is authoritative for new runs; callers and assignment prompts cannot raise it.
Fallback candidates use that node's effective access. Historical runs retain their original launch
ceilings. Network is a separate optional setting, subject to backend capabilities.
Publish authorizes remote publishing when the assignment requests it, including PR creation,
reviews and comments. It does not itself grant credentials or bypass harness approval rules.
Polybridge owns runtime execution, not GitHub operations. Agents use their own tools under the
user's harness settings. Polybridge injects no GitHub command approvals or publishing guidance.

| Freedom | Claude | Codex | Vibe |
| --- | --- | --- | --- |
| Read only | Plan mode; explicit git commit/push denies; no command allowlist added | Read-only OS sandbox; network blocked; no per-command allowlist | Plan profile; no command allowlist added |
| Write in repo | Accept-edits mode; explicit git commit/push denies; no command allowlist added | Workspace-write sandbox; no per-command deny list; network off by default | Accept-edits profile; harness/user Bash rules control commands |
| Publish | Accept-edits mode plus git commit/push allow rules only | Workspace-write sandbox; network on by default; no per-command allowlist | Auto-approve profile, equivalent to Unrestricted |
| Unrestricted | Bypass-permissions mode; no command allowlist or denies added | Full-access mode; no command allowlist; network cannot be blocked by this sandbox | Auto-approve profile |

These are Polybridge's mechanisms, not an exhaustive list of commands that can run. Inherited
harness/user settings, credentials, sandbox policy and network determine actual tool access.
Claude's explicit denies win over inherited allow rules; at Publish, other commands still follow
user permission settings. Codex does not mechanically forbid commits at Write in repo, and Vibe's
Publish mode is broader than commit/push. Permission reports describe these differences; the bridge
does not claim identical confinement. Native harness approval refusals retain their reason,
while ordinary command stderr is not classified as an approval refusal.

Codex reports additional workspace writable roots inherited from its user configuration and
selected profile, with the configuration source. These are observed settings, not an OS receipt;
relative paths stay as configured and a subsequent config change can affect launch.
Codex Publish enables its workspace sandbox's network access; Write in repo keeps network off
by default unless explicitly requested. Vibe and Antigravity provide no narrower publishing mode:
Publish currently uses their unrestricted approval mechanism, as reported in access caveats.
Polybridge does not claim identical confinement across harnesses.

Write-capable tasks receive a private scratch directory through `PB_TASK_SCRATCH`, outside the
repository. Claude and Codex receive that exact directory as an additional writable root;
read-only tasks receive no scratch grant. Each worker's internal preamble also names its exact
absolute scratch path so it need not discover the environment variable. This context is hidden
from the assignment display. Artifacts stay with the task record for inspection and
recovery, and task retention deletes them without following symlinks. The full display assignment
is stored in task metadata. Large Codex prompts use stdin with immediate EOF rather than argv,
so complete input results do not exceed operating-system argument limits.

For example, a Task step that updates a Jira issue can request `"freedom": "publish"`.
The saved node access is authoritative for new runs. Callers cannot override it with
`--freedom` or assignment fields. A parent harness that cannot delegate the configured
access receives a refusal before dispatch. Historical runs retain their original access
ceilings. Set network separately where the backend supports it.

Each agent step configures its instructions, primary candidate, fallback candidates, access,
session mode, and attempt budget. The orchestrator has its own candidate list, saved with the
workflow and overrideable when launching. Workflow generation has a separate candidate list.

MCP tools expose definition list/get/save/delete, `workflow_builder`, run list/start/status/wait,
inspection, recovery, and pause/resume/cancel. The Monitor acts through matching `polybridge-ctl workflow-*` commands;
it never writes Polybridge state directly. Definition saves use an expected revision to reject
concurrent edits. Editing or deleting a definition does not change existing run snapshots.

MCP run start/status/wait/control responses use `response_version: 1` compact projections,
with a 24 KiB target: identity, status, progress, interaction ownership, actionable question
previews and recent decision errors. They omit the graph, assignments and full result history.
`list_workflow_runs(offset=0, limit=10)` returns `runs` and `next_offset`; use the latter to
continue listing. CLI run snapshots remain complete for the Monitor.

Read complete run content with `get_workflow_run_detail(workflow_run_id, view, cursor, limit)`.
Views include `executions`, `decisions`, `checklist`, `technical_plan`, `definition`,
`builder_draft`, `generated_definition`, `question`, `reason` and `wait_reason`. Builder status
also includes the authoritative `draft_revision`, so a builder can resolve preview conflicts
without fetching a complete graph in every status response.
Concatenate each response's `chunk` until `next_cursor` is null, then decode JSON. Each chunk
is bounded, including a single large result. Cursors bind the run, view and content hash;
if content changes, restart that view. Managed orchestrators can read their own run only;
execution results still require settled-only `inspect_workflow_node` rather than a detail shortcut.

`wait_for_workflow` defaults to 30 seconds and caps its effective hold at 45 seconds.
It accepts legacy requests up to 300 seconds but returns `requested_timeout_seconds`,
`effective_timeout_seconds`, `timed_out` and polling guidance. Repeat the wait while the run
is active; a long request does not guarantee one long-lived transport connection.

For example, load the supplied [implementation/review workflow](../examples/workflows/implement-review.json):

```bash
polybridge-ctl workflow-save implement-review \
  --definition examples/workflows/implement-review.json --expected-revision 0 --json
polybridge-ctl workflow-start implement-review --repo /absolute/path/to/repo \
  --prompt "Implement the requested change" --json
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
is `agent_decides`, `resume`, `fresh`, or `continue_previous` (default `agent_decides`). Connections specify `source`, `target`, `condition`, and an optional
`default` fallback. Definitions use `routing_mode: explicit`. A `parallel_start` and its
`parallel_end` share a stable `parallel_group_id`. Polybridge derives retry direction from
topology; callers do not configure a backward flag. Historical snapshots retain their original
routing interpreter. Legacy `join` nodes remain readable in historical records. Nodes store top-left canvas coordinates in `position: {x, y}`. Coordinates must be finite and nonnegative; the canvas expands automatically. Snapping uses a 10-point grid. Background dots are visual guides: their spacing adapts to zoom in multiples of that grid, keeping at least 20 points between dots on screen. Only visible dots are rendered, and their screen size stays constant. Agent nodes are 200 × 92 points (reserve 20 × 10 cells); Start and End are 72 × 72 points (reserve 8 × 8 cells). Builder agents are guided to leave at least 40 points between node edges and preserve existing positions exactly unless the user explicitly asks to move or rearrange nodes.

Each role receives built-in role guidance, custom step instructions, the orchestrator assignment,
and explicitly labeled input results. Workers do not receive the original request, full graph, or
global checklist. The immediate predecessor result is attached automatically; converging parallel
branches supply all incoming results. The orchestrator can reference older settled results and
assign specific checklist tasks without exposing global checklist status. Planning creates pending tasks, implementation reports task IDs
and evidence, review gives a verdict and concrete findings, and task steps report observed
results. Only the orchestrator changes checklist statuses. Select Start to edit workflow
settings, including the orchestrator, fallback agents, and limits.

## Branches and attempts

Outgoing connections on Start, agent nodes, and Parallel end are **exclusive alternatives**. The
orchestrator chooses exactly one valid connection, or asks for input/stops when no justified path
is available. Choosing multiple alternatives is a protocol error: Polybridge dispatches nothing
and requests a correction within the decision-attempt allowance.

Parallel start explicitly opens a group and launches **every forward branch**. Its outgoing arrow
instructions describe branch purpose, not permission to omit a branch. Conditions for selecting
the whole group belong on its incoming arrow. Polybridge obtains focused assignments for ready
workers; structural traversal needs no assignment. `max_parallel` limits concurrent workers, so
a group remains valid even when its branches execute one at a time.

```mermaid
flowchart LR
    Scope[Scope and pin] -->|Inputs ready| Split[Parallel start]
    Scope -->|Inputs unavailable| End[End]
    Split --> Astra[Astra review]
    Split --> Claude[Claude review]
    Split --> Vibe[Vibe review: optional]
    Astra --> Merge[Parallel end]
    Claude --> Merge
    Vibe --> Merge
    Merge --> Adjudicate[Adjudicate and draft]
    Adjudicate --> End
```

Each branch may contain several steps and its own exclusive conditional routes. Every possible
forward route must reach the group's matching Parallel end. Sibling branches cannot overlap,
enter one another, escape the group, or reach workflow End before closing. Nested groups close
before their enclosing group; selecting either boundary in the canvas highlights its partner.

```mermaid
flowchart LR
    OuterStart[Parallel start: outer] --> X1 --> X2 --> OuterEnd[Parallel end: outer]
    OuterStart --> Y1 --> InnerStart[Parallel start: inner]
    InnerStart --> Y2 --> InnerEnd[Parallel end: inner]
    InnerStart --> Y3 --> InnerEnd
    InnerEnd --> OuterEnd
    OuterStart --> Z1 --> Z2 --> OuterEnd
    OuterEnd --> Next[Next step]
```

Parallel end waits for its own branches to resolve, combines their immutable result references,
and asks the orchestrator for the next continuation. An inner end can release while other outer
branches are still working. Arrivals are persisted by group generation and branch identity;
repeated arrival notifications cannot release a group twice. Required failures need recovery or
an explicit stopping/input decision; they cannot silently satisfy the barrier. A recovered branch
retains earlier failure evidence while its successful recovery resolves the barrier.

Retry loops may remain inside a branch, but cannot cross an open parallel boundary. Retrying a
whole closed group creates a new generation within the existing execution and transition budgets.
A retry arrow may share a step with forward alternatives, but selecting it remains exclusive.

Polybridge infers a retry when an arrow returns to a step that every path to its source passes
through. Canvas placement does not determine direction; ambiguous cycles are rejected.
Retry connections repeat a step. Workflow candidates default to a 100-turn cap where supported (Claude and Vibe).
Explicit limits are preserved; backends without turn-cap support, including Codex, do not
receive an unsupported cap.

Three attempts means the initial activation plus two repeats,
not three additional retries. New runs count this budget per graph visit: retries and malformed-output
corrections share a visit, while a later loop traversal starts a new visit. Loop limits remain
run-wide. Historical snapshots retain their original run-wide node budget. Defaults are three
activations per visit, 100 transitions per run, and four concurrently active node tasks. Read-only workflow steps share a checkout lease;
writing workflow steps require exclusive access. These leases coordinate workflow runs, not
manual edits or unrelated tools.

**Agent decides** lets the orchestrator choose Fresh, Resume, or Continue previous node for each assignment. Polybridge
issues compatible available session references in decision context. A Resume assignment names the
issued `resume_task_id`; arbitrary, busy, or incompatible sessions are rejected. **Fresh** starts
an independent conversation with only the assignment and selected input results. **Resume** keeps
an eligible candidate's prior conversation. Explicit node policies remain available. A different
fallback candidate starts fresh for a Fresh execution. When a selected Resume attempt encounters a
confirmed availability failure, Polybridge asks the orchestrator for an explicit Fresh decision
within the same node execution or clarification question. This happens automatically without asking
the caller for routine provider outages; Polybridge never silently replaces Resume with Fresh.
Unknown execution outcomes require reconciliation, and the orchestrator can still choose
`needs_input` when caller guidance is necessary.

For example, after a confirmed outage while resuming an asking worker, the orchestrator may answer
the same question with `session_mode: fresh`. Polybridge reconstructs the original assignment,
observed progress, and clarification history in the new session. For an ordinary node continuation,
the orchestrator selects the issued Fresh execution continuation with a new focused prompt. These
choices retain the activation identity and attempt history; they do not replay completed graph
steps.

**Continue previous node** (`continue_previous`) tries the session of the immediately preceding
serial agent execution, including its actual fallback candidate. Reuse requires one unambiguous
predecessor and compatible harness, model, effort, access, repository, and network settings.
Serial first entry never substitutes a historical predecessor session. On backward-edge loop
re-entry, Continue previous node uses the target node's own latest compatible settled session.
Retries use the selected execution's retained session. Convergence does not inherit a branch session. Incompatible or definitively unavailable predecessor sessions start Fresh; a
fallback candidate also starts Fresh. Retries retain their own execution session. The inspector
shows the actual mode and reason, including a specific backend, freedom, model, effort, or
network mismatch when reuse is unavailable. The orchestrator sees the same reason. Unknown or busy session outcomes require attention.

The inspector provides keyboard duration entry and a Seconds / Minutes / Hours selector.
Changing units preserves the limit; typed fractional durations round to the nearest whole second.

Agent nodes may set `timeout_seconds` to a positive integer; omitted, null, or zero disables it.
The deadline applies to each candidate execution. Expiry counts against the current visit's
attempt allowance. Polybridge cancels the task and its descendants, confirms settlement, and
records a timeout notice before using an ordered fallback. Unconfirmed cancellation pauses for
attention instead of starting overlapping work. The cancellation allowance includes SIGTERM
escalation and drain time. A recovered positively settled timeout still permits an unused fallback
in the same activation. Deadlines are persisted on each candidate reservation. Ordinary MCP
server restart does not stop the detached workflow supervisor; if that supervisor itself is lost,
surviving tasks require reconciliation and are not silently adopted or redispatched. A persisted
deadline provides diagnostics, not a watchdog that survives loss of the supervisor.

Planning nodes publish the latest complete technical-plan revision. Earlier versions remain in
immutable execution results for orchestrator inspection; a planning adjudicator that revises the
plan must return the full revised plan. Use a Task node for an arbitrary scoping/adjudication gate.
Review workers report `approved` or `changes_needed` when the review ran successfully; an inability
to perform the review uses worker `status: blocked` and does not require a review verdict. A custom
`pass`/`retry` gate may put that outcome in a Task result; the orchestrator still judges routing.

## Fallbacks

Fallback candidates are ordered and may include different backends or different models on the
same backend. Polybridge advances only after a confirmed availability failure: missing binary,
unavailable model, or a recognized quota/rate-limit rejection. Unsupported configuration is a
validation error. Failed tests, generic crashes, or an agent saying it is unavailable do
not automatically authorize a switch. An explicitly configured timeout permits a fallback only
after cancellation has confirmed settlement.

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
records and does not automatically replay uncertain dispatches. Checkout leases recheck live or
uncertain orphan task associations after acquiring the OS lock, closing the gap where a supervisor
can die between the initial scan and lock acquisition. Active and Needs attention runs
protect their referenced task records from ordinary retention. Finished run history retains compact
outcomes; expired activity logs are shown as unavailable.

## Delegation contracts and inspection

The orchestrator receives a durable decision ID, current stage, node responsibilities and access,
settled input results, checklist, recent decisions, and Polybridge-issued valid continuations.
Its JSON decision contains `decision_id`, `action`, and `reason`. Actions are `continue`,
`complete`, `failed`, `needs_input`, `inspect`, or `answer`. Continuing selects `continuation_id` entries and supplies
an assignment `prompt` for each agent execution, optional additional result references, and
optional assigned task IDs. Under Agent decides, each agent assignment also chooses
`session_mode: fresh|resume|continue_previous`; Resume identifies an issued `resume_task_id`. Structural traversal needs no assignment; convergence waits for every
branch before requesting one assignment for its next agent. Start always asks the
orchestrator for the first assignment. New runs retain one compatible native orchestrator session
across decisions, inspections, and worker questions. Each checkpoint still has its own durable
execution record and current authoritative context. A candidate change or a definitively missing
retained session boots Fresh; uncertain dispatch outcomes require reconciliation rather than
restarting the conversation or repeating work.

New runs avoid structural-only routing decisions. A continuation entering **Parallel start**
includes `branch_continuations`; the orchestrator returns a `branch_assignments` array containing
one separately written assignment for every issued branch. Branches can perform different jobs
and do not share a prompt. Polybridge validates the complete bundle before advancing or dispatching
anything, including nested groups. A single unconditional structural exit advances through normal
transition accounting. A successful worker with one unconditional path to End goes straight to
the completion decision, where the orchestrator still checks results and owns checklist updates.
Conditional paths, recovery, questions, and every new agent assignment still require judgment.
Parallel end already combines incoming results before one assignment decision for its next agent.

Decision examples are action-specific. New runs ignore unknown presentation fields and record
warnings alongside the accepted decision; empty assignment-reference fields on structural entries
are harmless. Invalid identities, continuation selections, required fields, nonempty misplaced
assignment context, and permission or harness overrides remain errors.

An agent continuation decision looks like this (IDs are issued by Polybridge):

```json
{
  "decision_id": "issued-decision-id",
  "action": "continue",
  "reason": "The plan is ready for review",
  "next": [{
    "continuation_id": "issued-continuation-id",
    "prompt": "Review the attached plan for missing cases and report concrete findings",
    "session_mode": "fresh"
  }]
}
```

Launch metadata and runtime evidence come through each backend adapter. Claude initialization
reports its native model. Vibe's matching native session metadata can report `active_model`, the
resolved model name, and configured thinking level, with `harness_session_configuration`
provenance and `observed_configuration` status. This proves the session configuration, not the
provider's actual served identity. Missing native metadata remains unknown; assistant self-reports
never establish model identity.

Workers return `{status, result, evidence}`. Status is `succeeded`, `failed`, `blocked`, or `asking`;
role-specific results retain plans, completed task IDs, review verdicts, and observed test outcomes.
Planning nodes independently configure **Require checklist** (`require_tasks`) and
**Require technical plan** (`require_technical_plan`), both true by default. A review-scoping node
can disable both and return a focused brief. If a checklist is required but the worker judges
that no implementation tasks are appropriate, it returns `no_checklist_needed: true` and a
nonempty `checklist_reason` instead of inventing tasks. This is explicit evidence for the
orchestrator to assess when deciding whether to continue, retry, fail, or ask for input; it never
marks existing checklist items completed. Even with one unconditional exit, this proposal gets
an orchestrator decision: it may accept the brief or select an issued replan retry within the
node attempt budget. Polybridge never retries the planning worker automatically.
Omitted optional outputs preserve the previously stored
checklist and technical plan. A normal planning result looks like:

```json
{
  "status": "succeeded",
  "result": {
    "tasks": [{"id": "parser-validation", "title": "Validate empty input", "description": "Preserve the existing public behavior"}],
    "technical_plan": "## Approach\nValidate input before parsing.\n\n## Verification\nRun the existing parser tests and add an empty-input case."
  },
  "evidence": ["Inspected the parser and its tests"]
}
```

Polybridge preserves the complete technical plan and its source execution. The immediate successor
receives both the plan and checklist descriptors in the planning result; the orchestrator owns
checklist completion. The right sidebar shows the plan and live checklist for each workflow run.

A reviewer finding issues or a test step reporting failing tests can still execute successfully.
Polybridge accepts one unambiguous JSON contract surrounded by prose or a code fence. Conflicting
objects remain protocol failures, with the complete raw output retained. New runs accept a complete
result envelope followed by unrelated text or surplus formatting; a missing or malformed envelope
still fails the protocol. Historical run snapshots retain their recorded runner policy.
For guided runs, a malformed final envelope triggers a focused correction turn on the same
harness, resuming its retained session when available. The turn explains the validation error
and asks only for a corrected result, without repeating the work or changing permissions. Each
correction consumes a node attempt; the original output and errors remain inspectable. When
the budget is exhausted, required failures return to the orchestrator for recovery and safe
optional branches may converge with their failure evidence. The Monitor shows a collapsible
“Malformed output” activity cell. The orchestrator conversation displays the original request
once, then brief checkpoint descriptions for later turns; internal decision guidance stays hidden. Explicit caller grants can reopen settled protocol retries
without granting additional tool access. An `asking`
result contains `result.question` and optional `result.context`. The worker yields; the runner
persists the question and marks its execution `waiting_for_answer`, without treating it as a final
result. The orchestrator returns `answer` with the issued question ID and a focused answer prompt.
Polybridge resumes that same execution's conversation; repeated questions remain recorded until a
final result. Each node defaults to ten clarification questions (`max_context_questions`),
so repeated questioning cannot run indefinitely. Answering does not create an extra graph activation. Parallel workers may ask
independently; questions are serialized through the orchestrator while already-running siblings
settle. No shared convergence advances until its selected branches produce final outcomes.

For example, a worker can ask for missing context:

```json
{
  "status": "asking",
  "result": {"question": "Which API behavior should remain compatible?", "context": "Two existing clients differ"},
  "evidence": ["Inspected both client implementations"]
}
```

The clarification decision uses the issued decision and question IDs:

```json
{
  "decision_id": "issued-clarification-decision-id",
  "action": "answer",
  "question_id": "issued-question-id",
  "answer": "Preserve the existing public API; change only the internal implementation",
  "reason": "The caller requested a compatible refactor"
}
```

Invalid decisions dispatch nothing. Polybridge returns the error and valid choices, with three
total decision attempts by default (Start setting, range 1–10). This allowance resets after a valid
decision and is separate from node retries. In new runs, exhaustion pauses scheduling with
`needs_attention` and preserves the last decision error in `attention_reason`. The caller can
resume with a correction; extra execution attempts remain an explicit grant. Historical snapshots
keep their earlier exhaustion behavior. Inspection does not consume
a decision attempt. Each decision checkpoint has a bounded inspection allowance (default 20,
configurable 1–1000); repeated inspection cannot indefinitely postpone a decision. `needs_input` includes a question and suspends scheduling; running siblings settle,
and status reports whether settling remains. Resume rejects live or uncertain dispatches.

The orchestrator controls the runner through its final JSON response, not by calling Polybridge
control tools. For inspection it returns `action: inspect`, its `decision_id`, a `reason`, and a
`requests` array of `{execution_id, task_id?, view?, cursor?, limit?, before_seq?, after_seq?}`.
The runner validates and serves those requests, then gives their bounded results back to the same
decision point. Harness tools remain available for other authorized work; no Polybridge MCP call
is required for workflow routing, answering questions, or inspection.

The public `inspect_workflow_node(workflow_run_id, execution_id, ...)` tool remains available to
ordinary callers and can inspect any fully settled node
execution in that run, including older executions and fallback attempts. `execution_id` is the
activation ID in run state. Optional `task_id` selects a particular candidate attempt. The result
view returns JSON text chunks with `chunk`, `next_cursor`, and `has_more`; concatenate the chunks
and decode JSON to read full `node_result` and `raw_output`. Pass `next_cursor` as `cursor` until it
is null. Result pages default to 16,000 characters and cap at 32,000. Cursors are bound to the run,
execution, selected attempt, and immutable content. The activity view uses normalized task events
with `before_seq`/`after_seq` pagination (50 events by default, maximum 200).

Managed orchestrators can read only their own run and settled node executions. Workers can read
only tasks from their own execution; workflow context reads are refused. Ordinary callers keep
existing read access. These are API boundaries, not filesystem isolation.

```bash
polybridge-ctl workflow-inspect RUN_ID EXECUTION_ID --view result --json
polybridge-ctl workflow-inspect RUN_ID EXECUTION_ID --task-id TASK_ID --view activity --json
polybridge-ctl workflow-recover RUN_ID --reason "Inspected and resolved the outage" --json
polybridge-ctl workflow-migrate --json
```

Interaction ownership is recorded when the run starts. MCP and ordinary CLI starts belong to the
**caller**: `needs_input` returns the question to that caller, and the Monitor shows the question
without offering an answer/control composer. Human Monitor starts use CLI `--monitor`; their
questions are answered through the Monitor. Caller-owned controls cannot impersonate Monitor
controls, and Monitor controls cannot resume, recover, or pause caller-owned runs. Explicit cancel
remains available independently. Monitor terminal continuation is offered only after the workflow
has completed its valid End traversal, never while a node is still participating in execution.

Resume of `needs_input` includes the exact published `input_decision_id` as `decision_id` (CLI
`--decision-id`) and the answer in `instructions`, preventing stale answers from being applied to a
later question. Monitor CLI controls also include `--monitor`. The caller's original request
remains the workflow prompt; answer prompts are stored separately for their individual task attempts.

If a supervisor encounters an exception while cancelling, Polybridge makes a bounded attempt to
settle its active dispatches, then records `needs_attention` with the cancellation error. It does
not repeatedly relaunch the failed cancellation supervisor. Existing task records remain
available for reconciliation; uncertain dispatches cannot be resumed or repeated.

Explicit `recover_workflow` requires a nonempty reason and a failed, settled run. It returns the
failed decision to the orchestrator with a fresh decision allowance and preserves completed work.

Decision exhaustion in new runs uses `needs_attention`, so use `resume_workflow` rather than
failed-run recovery. The last contract correction remains visible. Resuming renews the decision
allowance; `additional_attempts` grants execution/retry capacity only when explicitly requested.
A caller answer with an explicit attempt grant can reopen a settled blocked worker, including
missing-context blocks at End, within the granted budget. Its assignment includes the answer;
completed nodes are not replayed and saved access cannot be raised.
Already-running siblings finish and persist their results while scheduling is suspended. The
Monitor shows **Settling** until they finish and refuses premature resume or session takeover.
Extra execution/retry grants require `additional_attempts`; completed and cancelled runs cannot
recover. Caller answers and override reasons are passed back into orchestrator context.

This feature is unreleased. Conversion backs up and revision-checks saved definitions before
inserting explicit parallel boundaries around confirmed parallel regions. Original node IDs,
settings, positions, and arrow IDs are preserved; selected branch arrows and arrivals are rewired
through the new boundaries. Conditional alternatives remain outside the group. Ambiguous groups
require an explicit conversion choice rather than guessing from outgoing arrow count. Eligibility
checks previously used to omit an optional branch must move into that worker's instructions: an
ineligible worker reports blocked without performing the restricted work. This policy change must
be reviewed together with the converted definition. Conversion does not change session modes.
Historical records retain their execution snapshots. Active runs must settle or be explicitly
cancelled before cutover; migration does not cancel them automatically.

## Storage and permissions

Definitions live under `~/.polybridge/workflows/`. Immutable run snapshots, task associations,
decisions, and control history live under `~/.polybridge/workflow-runs/`. Ordinary agent streams
remain under `~/.polybridge/tasks/` and use the existing event format.

The orchestrator recommends decisions; it cannot grant additional node permissions. Builder and
orchestrator agents run read-only, and workers retain the run's permission limits. Existing backend
enforcement caveats still apply: a workflow does not create an OS sandbox for a backend that lacks
one. Inspect each task's enforcement report.

## Unsaved editor drafts

The Monitor automatically keeps unsaved canvas edits as local drafts, including incomplete graphs and applied agent proposals. Navigating away or reopening the app restores the draft. Drafts retain the original saved revision, so Save still detects another update to the saved workflow. Only an explicit successful Save changes the runnable definition. Save is disabled when the canvas matches the saved workflow.

A workflow editor draft also remembers its builder conversation. Navigating to another task and back reconnects to the same running agent or completed proposal. Applying the proposal or explicitly returning to the canvas dismisses that conversation from the editor. Workflow-builder activity has no terminal takeover button.

New canvases start with Start → Plan → Implementation → Review → End in an evenly spaced row. Review also has a retry path back to Implementation when changes are needed.

With the canvas focused, use ⌘C and ⌘V to copy and paste selected nodes. Pasted nodes get new IDs and an offset; connections within the selected group are copied. Existing Start/End nodes are skipped so terminal nodes stay unique. Text fields retain their normal copy/paste behavior.

With the canvas focused, press ⌘Z repeatedly to undo earlier edits. A full node drag is one undo step. Text fields retain their normal text undo. **Discard Changes**, beside Save, restores the saved workflow (or the starter for a new workflow) and clears its unsaved draft. Discard and canvas undo are unavailable while an agent is editing.

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
Only the verified active builder task can publish a preview. Render-safe previews accept
Fresh, Resume, and Agent decides sessions and validate both planning output flags. An idle live
builder reads pending feedback without rewriting run state; state is persisted when feedback is
actually claimed or delivered.

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

Node prompt descriptions, initial activity bubbles, and Prompt tabs show the exact orchestrator
assignment for that execution, including distinct assignments on retries. The overall workflow
continues to show the caller's original request. Internal role guidance, input payloads, and protocol
instructions are stored separately from that display projection. Raw diagnostic backend logs may
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
the overall workflow shows that request, while each node shows its orchestrator assignment.

A candidate can also fall back when Polybridge positively refuses a harness capability before
starting its process, such as an unsupported turn cap or reasoning control. An unsupported primary
configuration is accepted only when an ordered fallback exists. Each fallback keeps the same
effective access and network limits. Security lineage refusals, repository/session problems,
ordinary permission denials, and failed checks require attention rather than bypassing restrictions.

Ordered fallbacks also cover authoritative provider outages: failed harness API/transport
diagnostics or typed provider errors identifying server failures, overload, or connection failures.
Silence, assistant/tool claims, and ordinary test failures do not trigger fallback.

Agent nodes may set `optional: true` (default `false`) to tolerate a definitive failure inside an
active parallel branch. The step still runs and tries its configured fallbacks. If it fails, its
branch arrives at the current convergence with explicit failure evidence; a required sibling must
belong to the group. An optional step cannot bypass required steps before that Parallel end.
Sequential optional nodes and forks containing only optional branches are rejected. Selecting an
optional path alone does not grant failure tolerance. Unknown outcomes, cancellation, security or
enforcement refusals, repository/session problems, and workflow limits still require attention.
A positively settled timeout may bypass an optional branch even if earlier tool calls were denied;
the timeout and all permission denials remain in its failure evidence. A failure or block caused by
a permission refusal remains ineligible. Parallel-end arrivals are offered only when their required
failure evidence is resolved; otherwise the orchestrator receives the blocker and available recovery
choices. Reloading and resuming an older paused run rechecks this eligibility.
Unsupported model, turn, or reasoning settings count as unavailable candidates after fallbacks.

### Explicit retries and task navigation

A safely settled failed or blocked worker can expose a `retry_execution` continuation
while its branch remains at that decision point and its node has attempts remaining.
The orchestrator supplies a corrected assignment and an allowed session choice. This
consumes a node attempt without traversing a graph edge or repeating completed siblings.
Protocol failures, permission failures, cancellation and uncertain dispatches do not
silently retry. Structural continuations accept only their continuation ID; session
fields belong to executable assignments.

Polybridge records requested and effective harness settings separately from observed
runtime identity. Workers do not need to certify a model or effort they cannot inspect.
Only a returned review verdict counts as a review; a blocked result is evidence of a
blocker, not an approval.

The sidebar groups workflow and parallel executions under expandable parents. Opening
a child from the overview selects its full task details. Resumed turns share a child only
when they belong to the same harness session; fresh sessions stay separate, including
orchestrator sessions. The overview focuses on live
activity; full decoded results remain in individual task details. Workflow tasks cannot
be taken over while their parent run is active, suspended or still settling.

### Recordless dispatch reconciliation

Reservations record whether Polybridge is still preparing or has requested a spawn. A restart
can settle a recordless preparing reservation as not started. Once spawning was requested,
missing records remain uncertain: they never expire automatically or permit redispatch.
Known exec failures before a process exists are recorded as not started.

After independently confirming that no process survived, a human can release a named
recordless reservation with:

```sh
polybridge-ctl workflow-abandon-dispatch RUN_ID EXECUTION_ID TASK_ID \
  --reason "Confirmed the process is stopped" --confirm-no-process
```

The command refuses agent callers, live or uncertain supervisors, recorded tasks and unrelated
reservations. It persists the confirmation and reason without scheduling work or granting
attempts. Resume or recovery remains a separate explicit action.


### Monitor polling and complete detail retrieval

Monitor polls `workflow-status RUN_ID --monitor-view --json` for bounded metadata and content
digests. When content or state changes, it captures one coherent transport snapshot using
`workflow-status RUN_ID --monitor-view --snapshot --json`. Subsequent `--cursor CURSOR` calls
read the same immutable snapshot in bounded 128 KiB JSON chunks, so a running workflow cannot
invalidate an in-progress load. Completed paging deletes its temporary snapshot; abandoned
snapshots expire after five minutes. These files do not update saved workflows or run records.

The window coordinator retains the last loaded content for up to eight runs. Reopening a run
shows that content immediately while refreshing; a failed refresh preserves it. Unchanged polls
transfer metadata only. Changed snapshots currently transfer the complete run, rather than
individually fetching old execution results. This trades some bytes for coherent loading and
far fewer CLI subprocesses.

`workflow-detail RUN_ID --view VIEW --cursor CURSOR --json` remains available for lossless
individual views, including definitions, checklists, plans and execution history. Its content-bound
cursors still require restarting when their view changes. Ordinary `workflow-status` retains its
full response contract. Snapshot transport is a local Monitor-only CLI operation: managed agents
cannot use it to bypass settled-node inspection or access another run.
