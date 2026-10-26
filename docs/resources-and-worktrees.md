# Resources

Named, exclusive resources let tasks take turns on something only one of them can use at a time: an
iOS simulator, a test database, a device, a branch. Resources grant nothing and change no access;
they only order who may start. (Task worktrees are documented here when they ship.)

## Using resources

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
  MCP server and `harflow-ctl`). Workflow launches do not accept them yet.
- The names are released when the run's exit is observed. A grandchild process that outlives the
  agent (a simulator, a dev server) can still be running; cleanup hooks are future work.

## A task that is waiting

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
  the owner honours at its next poll, and cascades to pending descendants. A launch that has already
  spawned is cancelled the ordinary way.
- After spawn the task record carries `resources {names, reservation_id, state, wait_started_at,
  granted_at, released_at, borrowed_from?}` in `get_task_status` and `harflow-ctl status`.
- `harflow-ctl status` and `status-batch` answer a pending id with the additive error code
  `task_pending_launch` and `launch_state` in `error.data` (`data` in a batch row); `list` is
  unchanged. `harflow-ctl resources --json` shows every name with its holders and queue.

## Resume, continue and follow-ups

`resume_task`, `continue_task` and automatic continuation inherit the task's `resources.names` and
**re-queue**; they never inherit a held reservation, so there is a gap between a task's release and
its successor's grant in which another task can take the names. A resume or continuation that has
to wait is acknowledged the same way (`launch_state: "waiting_for_resources"`); a continuation's
reservation is recorded as `accepted`, distinct from `started`.

Queued follow-ups consumed by a continuation that waits are handed to its launch owner. They are
marked consumed when the task actually spawns, and queued again if the launch never starts (the wait
expired, it was cancelled, or its owner died while it was still waiting). If the owner dies after the
grant and before a record appeared, the delivery is `delivery_unknown`.

## Nested tasks: declare at the top

A task that runs another task through Harflow is a *holding scope* when it holds resources. Its
children may only ask for names it declared, and they are granted at once as a **borrow** (no
capacity used, no queueing); any other name is refused with `resource_not_declared_by_scope`. Declare
every resource at the outermost task. The rule is what keeps waits from forming a cycle: nothing
that holds a resource ever waits for another one. Borrowers share the lender's names, so mutual
exclusion *inside* one scope is the scope owner's job. A lender's release is deferred until its last
borrower is done. A task started from inside a workflow attempt cannot request resources in this
release. When the caller cannot be identified, the request is refused.

## Recovery

- The reaper frees an entry whose owner is provably finished or never started: a task record that
  is conclusively settled, a launch marked `not_started`, or a waiting launch whose owner is dead.
  It never frees on uncertainty: a launch `granted` with no record, an owner that cannot be
  inspected, or an agent that may still be alive. It runs in the server's maintenance pass and
  whenever a launch is waiting.
- `harflow-ctl resources-release --reservation-id ID --confirm-uncertain [--also-release-session]`
  is the person's decision for an entry the reaper cannot settle. `--also-release-session` also
  settles the launch of a `granted` task that has no record, so its session is free to resume.
  Both recovery commands refuse any detected agent caller and fail closed when that cannot be
  decided.
- A corrupt ledger makes every resource operation refuse. `harflow-ctl resources-repair` moves it to
  `ledger.corrupt-<timestamp>.json` and rebuilds the holders it can prove from durable state
  (unsettled task records and `granted` launches). Waiting launches, borrowers and anything
  unprovable are not restored: check what is running before starting work that needs a name.

## Guidance for agents

Paste into an agent's instructions where it dispatches Harflow tasks:

> If work needs a shared, exclusive thing (a simulator, a device, a database), pass it as
> `resources` when you start the task. Declare every resource at the outermost task that needs it;
> a task you start from there may only use names you hold. A task waiting for resources reports
> `launch_state: "waiting_for_resources"`: keep waiting rather than starting it again.

## Limits

Capacity is one and not configurable. A grandchild that outlives its agent can outlast the release.
Resources do not isolate anything: two tasks that do not name a resource can still collide on it.
The Mac Monitor does not show pending tasks yet; it can read them from `harflow-ctl resources --json`.
