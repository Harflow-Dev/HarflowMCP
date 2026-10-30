# Resources and worktrees

Two independent options on dispatched tasks and workflows: named exclusive [resources](#resources) and a private
[worktree](#worktrees). They combine: resources are granted first, then the worktree is made.

## Resources

Named, exclusive resources let tasks take turns on something only one of them can use at a time: an
iOS simulator, a test database, a device, a branch. Resources grant nothing and change no access;
they only order who may start.

### Using resources

```text
start_task(prompt, repo_path, resources=["ios-simulator"], resource_wait_seconds=1800)
harflow-ctl run --backend claude --repo . --prompt "..." --resource ios-simulator --resource-wait-seconds 1800
```

- A name is 1 to 64 characters of lowercase letters, digits and `. _ : -`, starting with a letter or
  digit. A request names 1 to 16 distinct names. Capacity is one per name.
- A task starts only when it holds **every** name it asked for, never some of them. Requests are
  granted strictly first come first served per name, so a request for several names cannot be
  starved by a stream of small ones.
- `resource_wait_seconds` is 0 to 21600 (default 3600). 0 means grant now or fail. A task that waits
  out its bound never starts: it reports `status: "failed"`, `not_started: true` and
  `not_started_reason: "resource_wait_expired"`.
- Resources are accepted at every freedom, including `read_only`, and need supervised execution (the
  MCP server and `harflow-ctl`). Workflows declare resources in their saved definition; see [workflow scopes](#workflow-scopes).
- The names are released when the run's exit is observed. A grandchild process that outlives the
  agent (a simulator, a dev server) can still be running; cleanup hooks are future work.

### A task that is waiting

A task with resources is acknowledged at once and waits in its detached owner. It has **no task
record** until it spawns, so nothing that reads a record sees it, and the call returns
`status: "running"` with `launch_state: "waiting_for_resources"`.

- `get_task_status` and `wait_for_task` (and `wait_for_tasks`) report the wait: `resources` has the
  `names`, `state` (`waiting`, `granted`, `not_started`), `position` in the queue, `blocked_by` (the
  holders per name), `wait_started_at` and `deadline_at`. A wait counts as running. `launch_state`
  is `uncertain` when its owner cannot be inspected; its resources are then kept.
- `list_tasks` includes pending launches (`pending_launch: true`) and takes
  `waiting_for_resource=NAME` to list those queued for one name. The indexed page (`group`,
  `limit`, ...) does not include them.
- `cancel_task` (and `harflow-ctl cancel`) on a pending launch writes a pre-spawn cancel request that
  the owner honours at its next poll and again at the last point before the agent starts (after the
  grant, the worktree and the pre-spawn checks), and cascades to pending descendants. A request that
  lands after that point is applied by the owner right after the spawn, through the ordinary cancel.
- After spawn the task record carries `resources {names, reservation_id, state, wait_started_at,
  granted_at, released_at, borrowed_from?}` in `get_task_status` and `harflow-ctl status`.
- `harflow-ctl status` and `status-batch` answer a pending id with the additive error code
  `task_pending_launch` and `launch_state` in `error.data` (`data` in a batch row); `list` is
  unchanged. `harflow-ctl resources --json` shows every name with its holders and queue.

### Resume, continue and follow-ups

`resume_task`, `continue_task` and automatic continuation inherit the task's `resources.names` and
**re-queue**; they never inherit a held reservation, so there is a gap between a task's release and
its successor's grant in which another task can take the names. A resume or continuation that has
to wait is acknowledged the same way (`launch_state: "waiting_for_resources"`); a continuation's
reservation is recorded as `accepted`, distinct from `started`.

Queued follow-ups consumed by a continuation that waits are handed to its launch owner. They are
marked consumed when the task actually spawns, and queued again if the launch never starts (the wait
expired, it was cancelled, or its owner died while it was still waiting). If the owner dies after the
grant and before a record appeared, the delivery is `delivery_unknown`.

### Nested tasks: declare at the top

A task that runs another task through Harflow is a *holding scope* when it holds resources. Its
children may only ask for names it declared, and they are granted at once as a **borrow** (no
capacity used, no queueing); any other name is refused with `resource_not_declared_by_scope`. Declare
every resource at the outermost task. The rule is what keeps waits from forming a cycle: nothing
that holds a resource ever waits for another one. Borrowers share the lender's names, so mutual
exclusion *inside* one scope is the scope owner's job. The scope is the nearest task on the
caller's `spawned_by` chain that holds a reservation (a grandchild is covered through a child that
holds nothing); an ancestor that cannot be read refuses the request. A lender's release is deferred
until its last borrower is done. A workflow attempt is always a holding scope, even with no named resources; an empty scope refuses every name. Workflow-managed agents remain subject to the direct-dispatch guard. When the caller cannot be identified, the request is refused.

### Workflow scopes

Saved definitions may declare top-level `resources: ["ios-simulator"]` for the run and
`resources` on connected or on-demand agent nodes for one candidate attempt. Run names remain
held through `paused`, `needs_input` and `needs_attention`, until terminal settlement and resolution
of every attempt and borrower. Node names must be a subset when the run declares resources;
they borrow the run's hold. Without a run scope, each node attempt queues independently before
occupying a harness slot or checkout lease. Siblings borrowing one run scope share its names;
use separate attempt scopes when siblings must exclude one another. Resource waits have a
3600-second bound; expiry leaves the attempt not started and the workflow needing attention.
Pause or cancel stops a queued attempt without dispatching it.

`prefer_subagent` nodes cannot declare attempt resources. Native and headless nodes without
attempt names have the same run-scope resource coverage. Run-workflow nodes take no resource
field; linked children borrow from their enclosing run scope. Resource names grant no filesystem,
network or publication access.

A standalone task with a holding scope may call `start_workflow` only when the run and every
connected or on-demand node in the pinned dependency tree request names covered by that scope.
An uncovered name is `resource_not_declared_by_scope`. Workflow-managed agents still cannot
launch independent workflows or tasks directly; saved workflow nodes use the shared scheduler.
Internal workflow-attempt lineage is a scope even when empty, so a descendant cannot queue for a
new resource while its ancestor holds a checkout lease.

Compact status keeps `resource_wait` as an object for the run-level wait and exposes
`resource_waits` as an array of up to eight active activation waits, each with names and blockers.
`resource_held_waiting_for_slot` reports an active named holder blocked on an intra-tree slot. Paged execution
views retain each candidate's `resource_attempt` and its queued, granted, not-started or attached
state. Monitor snapshots expose these fields; a native Mac UI for them is separate work.

### Recovery

- The reaper frees an entry whose owner is provably finished or never started: a task record that
  is conclusively settled, a launch marked `not_started`, or a waiting launch whose owner is dead.
  It never frees on uncertainty: a launch `granted` with no record, an owner that cannot be
  inspected, or an agent that may still be alive. It runs in the server's maintenance pass and
  whenever a launch is waiting.
- `harflow-ctl resources-release --reservation-id ID --confirm-uncertain [--also-release-session]`
  is the person's decision for an entry the reaper cannot settle. `--also-release-session` also
  settles the launch of a `granted` task that has no record, so its session is free to resume.
- `harflow-ctl launch-settle --task-id ID --confirm-uncertain` uses the same dead-generation
  recovery as maintenance. Both saved owner and agent identities must prove dead; a missing agent
  record for a potentially spawned launch reports `agent_identity_unavailable`. Human confirmation
  bypasses only a tasks-directory binding mismatch. Proven pre-spawn worktree failures may settle
  without an agent record, as may a waiting-for-resources owner confirmed dead. A fully terminal
  receipt with matching terminal task/agent evidence can repair directory binding only, returning
  `binding_repaired` while preserving state, outcomes and claims. Terminal receipts without that
  evidence remain refused. Follow-ups stay `delivery_unknown` unless execution provably never began.
  Recovery writes interruption evidence before releasing claims and retains pending reconciliation
  across crashes. It never signals a process or invents an exit/drain receipt.
  `resources-release` remains a separate manual resource/session-claim judgment, not proof of task
  settlement or permission for automatic continuation. Recovery commands refuse agent callers and
  fail closed when caller identity cannot be decided.
- A corrupt ledger makes every resource operation refuse. `harflow-ctl resources-repair` keeps it as
  `ledger.corrupt-<timestamp>.json` and closes the ledger (every operation reports
  `resource_ledger_repairing`) while it collects the holders it can prove from durable state
  (unsettled task records, `granted` launches, workflow run holds and granted or attached workflow attempts) and the borrowers that depend on them, then
  publishes them all in one step. A lender with a live borrower stays held (`release_requested`
  once the lender itself is done); one that cannot be found is replaced by an entry that blocks its
  borrowers' names until they finish or you release it. The same applies when the lender
  conflicted with another restored holder over a name: a borrower is never narrowed to the names
  the other holder did not win, so every name it held stays blocked until it finishes. If the command is interrupted the ledger
  stays closed: run it again. Queued workflow attempts and waiting task launches are not restored (their owners settle them
  `not_started`) and neither is anything unprovable: check what is running before starting work
  that needs a name. Workflow grants are confirmed after persistence and before dispatch, so a repair that collected earlier queued state cannot erase a grant unnoticed. Read-only workflow resolvers never reconcile or free a run merely because its supervisor died; explicit scheduler recovery reconciles pre-dispatch attempts.

### Guidance for agents

Paste into an agent's instructions where it dispatches Harflow tasks:

> If work needs a shared, exclusive thing (a simulator, a device, a database), pass it as
> `resources` when you start the task. Declare every resource at the outermost task that needs it;
> a task you start from there may only use names you hold. A task waiting for resources reports
> `launch_state: "waiting_for_resources"`: keep waiting rather than starting it again.

### Limits

Capacity is one and not configurable. A grandchild that outlives its agent can outlast the release.
Resources do not isolate anything: two tasks that do not name a resource can still collide on it.
The Mac Monitor does not show pending tasks yet; it can read them from `harflow-ctl resources --json`.

## Worktrees

```text
start_task(prompt, repo_path, freedom="write_in_repo", worktree={"base_ref": "main", "branch": "fix/login"})
harflow-ctl run --backend claude --repo . --prompt "..." --worktree [--worktree-base REF] [--worktree-branch NAME | --worktree-detach]
```

The task runs in `<worktree_root>/<task_id>`, a linked Git worktree of `repo_path`, so it can write
without touching your checkout or index. The root is `HARFLOW_WORKTREE_ROOT` (an absolute path;
default `~/.harflow-worktrees`). It must lie outside Harflow's state directories and outside the
source repository (neither may contain the other), or the request is refused.

- `base_ref` defaults to the repository's `HEAD` and is resolved to a commit by the launch owner
  *after* any resource grant, so a task that waited is based on what the ref names then. `branch`
  defaults to `harflow/<task_id>` and must not exist; `detach: true` checks the commit out with no
  branch (and excludes `branch`). Git's own refusal to check a branch out in two places stands.
- The call returns once the launch is accepted, typically as `launch_state: "granted"` while the
  worktree is made, and the task has no record until it spawns. The pending view carries
  `worktree {path, source_repo_path, base_ref, branch, detached, base_commit, state}` with state
  `planned`, `creating`, `created`, `failed`, or `unknown` when the owner died inside the Git command.
- After spawn the record's **`repo_path` is the worktree**: it is the working directory, the base of
  the backend argv and identity markers, the session store key, and what resume, continuation,
  approvals and extra paths use. An additive `worktree` object keeps `source_repo_path`, `base_ref`,
  `base_commit`, `branch` and `detached`. The path is deterministic, so the invocation is first built
  against the planned path and then rebuilt by the owner once the checkout exists (command grants and
  approval configuration read the real tree).
- Resume and continuation run in the same worktree. If it has been removed they refuse with a message
  that says so.

Safety: the request is refused at `read_only` (creating a worktree writes the source repository's Git
directory), for any nested or undetermined caller (a confined parent must not make Harflow write its
`.git`), in the in-process registry, and when the repository configures Git filter drivers
(`filter.*.clean|smudge|process`, so Git LFS is unsupported). Creation runs Git with
`core.hooksPath=/dev/null` and `core.fsmonitor=false`, so no repository hook (including
`post-checkout`) or monitor program runs; it never uses `--force` or a reset and leaves your
checkout, index and untracked files alone. If creation fails, the launch settles `not_started`
(`worktree_failed`), resources are released and a partial directory is reported but never removed.

Cleanup is explicit and for a person only (`lineage.detect_caller_detail`, failing closed):

```text
harflow-ctl worktree-list --json
harflow-ctl worktree-remove --task-id ID  # task ID or root workflow run ID
```

`worktree-remove` takes the per-worktree usage lock (`<root>/.locks/<id>.lock`, which every launch into
the path also takes briefly before writing its receipt), refuses while any task, launch or workflow run using the path
is unsettled or uncertain (a task running in a subdirectory of the worktree counts), and refuses unless the tree is clean; it also refuses a worktree with an initialized submodule (including one whose working directory is empty but whose Git directory exists under `<common>/modules/<path>`), a populated submodule, and one locked with `git worktree lock` (`worktree_locked`). Everything is verified again immediately before the removal, and `git worktree remove --force` then runs under hardened Git (no global or system configuration), only so that Git does not run its own status inside the agent's tree under the tree's configuration. The residual window is a stray process of the agent writing between the final probe and the removal. The tree belongs to an agent that may have rewritten its `.git` file or configured
a program to run, so removal never trusts it: it first proves that `.git` names a directory directly
under the source repository's `worktrees/` and that the `commondir` and `gitdir` back-links agree
(otherwise `worktree_link_invalid`), refuses if the shared or the worktree's own configuration defines
Git filter drivers (`worktree_filters_unsupported`), and reads the status through the same
configuration-free, pinned Git directory that task outcome capture uses, so no fsmonitor, filter,
hook or external diff program configured in the repository runs in your process. The branch is never deleted. Retention never
deletes the record of a task whose worktree still exists.

Limits: Git LFS and other filter drivers; Codex gains nothing from a worktree at `write_in_repo` or
`publish` (its sandbox denies Git directory writes there as in a normal checkout; see
[backend CLI facts](backend-cli-facts.md#codex-and-linked-worktrees-codex-cli-0163-0-2026-10-27)); a
worktree-only launch whose owner dies after the worktree is `created` keeps its session claim until a
qualified dead-generation recovery settles it; workflow worktrees follow the same Git and permission limitations; `notify` is not supported with `worktree`.


### One worktree per workflow

`start_workflow(name, prompt, repo_path, worktree={"base_ref": "main"})` or
`harflow-ctl workflow-start NAME --repo . --prompt "..." --worktree` creates one worktree at
`<worktree_root>/<run_id>`, with default branch `harflow/workflow-<run_id>`. The supervisor creates
it after the run-scope grant and before any dispatch. Every node and linked child run uses that
execution path; the run's `source_repo_path` and `worktree` metadata retain the original checkout.
Task shared scratch and system guidance continue to use the existing launch-owner paths.

`preview_workflow_run(..., worktree={...})` and `harflow-ctl workflow-preview ... --worktree`
disclose the plan. Resources, worktree options and root are included in the preview hash. Requests
are refused for nested or undecidable callers and when every agent in the pinned tree is
`read_only`. A crash during creation leaves an uncertain retained path and requires inspection;
it is never automatically retried or deleted. Non-terminal and unsettled workflow runs prevent
removal even between node tasks. Pass the root run ID to `worktree-remove --task-id`.

### Workflow recovery boundaries

A standalone task holding resources can start a workflow only when it covers every resource
in the pinned dependency tree, including on-demand nodes. Before launch returns, the workflow
publishes and confirms its own borrow anchor. With no explicit run scope, the runtime-only
`resource_anchor_names` is the union of those declarations; the saved definition is unchanged.
The anchor survives its caller's exit. A positive initial supervisor launch failure releases the anchor
and leaves the run needing attention; resume must borrow again from the recorded original lender,
which must still be held and not release-requested. It never falls back to an independent queue.
A failed replacement supervisor retains established holds while descendants may remain active.
Workflow-managed agents cannot directly start independent tasks or workflows; saved Run workflow
nodes provide linked child execution.

Reapers read persisted workflow state only. Dead queued owners and stale task outcomes remain
conservative blockers until explicit workflow recovery or cancellation reconciles them. This can
block later FIFO waiters. Candidate waits default to 3600 seconds; an explicit retry or fallback
starts a new bounded wait and loses its former FIFO position. Ledger corruption, repair, or a lost
queued reservation stops scheduling with a durable not-started attempt and attention; no candidate
fallback or optional bypass is inferred. Repair does not reconstruct queued requests.

A restart during workflow worktree creation changes `creating` to `unknown`, keeps resources held,
and requires attention. Cancel the run, wait for settlement, then inspect `worktree-list` and remove
a clean retained checkout explicitly by its run ID. Creation is never automatically retried.
`worktree-list` workflow rows identify their owner with `workflow_run_id`; task rows keep `task_id`.

Preview and start validate resources along every pinned invocation path. A child declaration may
narrow its nearest enclosing run declaration but cannot add names; a child without a declaration
inherits that scope, including for its on-demand nodes. Definitions reused under multiple parents
are checked independently for each path.

Ordinary launch containment checks read small qualified workflow-worktree lookup receipts in
`workflow-worktrees/` under the selected state root. Registration is durable before a root run is
published or Git creates its checkout. Orphan receipts remain conservative path hints; they never
authorize execution or removal. Explicit inventory and removal still read qualified full runs and
refuse when workflow state is unreadable.

External caller coverage is only acquisition provenance: a workflow with no declared names and
no acquired anchor exposes an empty scope, even if its caller holds other resources. Linked children
refresh their parent's current held scope rather than relying on an older saved reservation hint.
When a named root needs to reacquire, it can borrow only from its recorded original external lender.

Workflow run publication finds existing checkout locks through small task launch plans and workflow
lookup receipts. If a task launch file was removed before its task metadata during partial retention,
a targeted read of valid task IDs in the requested path's ancestors preserves record-only worktree
locking, including nested paths and paths under an older configured worktree root.

Lookup receipts are retained after worktree removal. Corrupt engine-owned lookup metadata fails
closed; G04 does not automatically prune or repair these receipts. A missing worktree may therefore
remain visible in inventory without triggering creation or cleanup.
