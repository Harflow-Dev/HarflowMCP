# Architecture

Maintainer-facing detail behind the rules in `AGENTS.md`: the backend and client seams, recovery and
live-input behavior, takeover and ctl, enforcement claims, and how state changes are reported to the
caller. The CLI measurements these rules rest on are in [backend-cli-facts.md](backend-cli-facts.md).

## Architecture: everything agent-specific lives behind `Backend`

`backends/base.py` defines the contract; `backends/claude.py`, `backends/codex.py`,
`backends/opencode.py`, `backends/vibe.py` and `backends/antigravity.py` implement it. **Nothing outside `backends/` may branch
on a backend's name.** If you find yourself writing `if backend == "codex"`, the seam is missing a
method.

Each backend supplies: argv builders (returning an `Invocation` — argv plus stdin wiring and any
initial stdin bytes), `assert_safe` (over the whole `Invocation`), `enforcement`, `ingest` (normalise
its stream into `Accumulator`), `normalize`, `classify` (decide the terminal status from its own
signals), `encode_live_message` (one message in its CLI's live-input format, or
`UnsupportedCapability`), and `interactive_resume_argv` (the command a human runs to resume the
session in the CLI's own UI — built at takeover time, never stored, None when unsafe). Adding a
backend should mean one new module plus a registry entry — nothing else. That held when `opencode`
was added: the only non-`backends/` changes were docs, the places that enumerated the two names, and
tests. It held again for `vibe`: no change to the `Backend` protocol was needed, even though vibe
rejects both `model` and `reasoning_effort` outright — that only needed a `supports_model_selection`
capability field beside the existing `reasoning_effort` one, not a protocol change. `antigravity`
was the fifth, and the first since claude with live input. It needed one more capability field,
`live_input_message_is_turn`, because agy runs every written message as its own turn where claude
folds a mid-turn message into the turn already running. The pump in `tasks.py` reads that field,
never the name. If your next backend needs more than a module, a registry entry and a declared
capability, the seam is missing a method.

`Capabilities` exists so callers are told, not surprised. Unsupported requests **fail loudly**:
`max_turns` on Codex raises rather than being dropped, checked both in `server.py` and again in the
backend itself.

`Invocation.env` is the only backend-specific child environment contribution. Every backend
validates its complete mapping in `assert_safe`; `_spawn` overlays the accepted mapping onto a copy
of the selected child environment before applying task lineage and scratch authority. Vibe sets
`VIBE_CLI=python` on both argv and one-shot stdin prompts. Interactive takeover is user-owned and
inherits the user's environment.

`artifact_configuration_namespaces(repo)` declares backend-owned configuration paths excluded
from task artifact references. Keep it separate from execution qualification: inline configuration
that prevents certifying a writable launch is not itself a file namespace. Registration is
bridge-owned metadata bookkeeping, not a new backend execution or filesystem permission.

## Installing is a second seam: `clients/`

`backends/` is *who we dispatch to*. `clients/` is *who can launch us* — the Claude desktop app,
Claude Code, Codex, opencode, vibe. Same rule: nothing outside `clients/` branches on a client's name,
and adding one should mean one module plus a registry entry.

The two kinds share only the interface. The desktop app has no CLI, so `desktop.py` edits its JSON
(merge, back up, atomic replace). The other four own config formats we have no business
reformatting — 78 KB of application state, hand-commented TOML, JSONC, and TOML again — so they are
driven through their own `mcp add`, which stores the entry in each one's own shape.

`Result.status` is seven values, not two, for the same reason `Enforcement` is strict: `unknown` exists
because a CLI that times out may already have written the config, so `failed` there would be a guess
stated as a fact. `removed` and `not_installed` are the two uninstall outcomes that did not go wrong,
kept apart because only one of them changed anything. And success is reported as "add command
succeeded", never "registered" — exiting zero is all that was observed.

Every client implements `inspect(key, run)` and `remove(key, run)` as well as `apply`. Both take the
server's key, not a `Registration`, because neither may need the server binary — an uninstall has to
work after it is gone. `Inspection.installed is None` means "could not tell", never "not installed".

## Invariants — break these and the design stops holding

1. **Only `_monitor` publishes a terminal status and sets `Task.done`**, after process exit *and*
   pipe drain. Cancellation sets `cancel_requested` and lets the monitor decide. The one status it
   must *not* publish is one for a process still alive: on its own `CancelledError` the server is
   being torn down, not the run, so it leaves the task `running` and re-raises — unless a
   cancellation is already in flight, whose intent nothing on disk could reconstruct. See "When the
   client restarts the server".
2. **Bookkeeping must never change an outcome.** A stale attribute in a *log line* once turned a
   successful run into `failed`, because the exception escaped into the monitor's handler. That log
   call is now individually guarded.
3. **Signal `Task.pgid` captured at spawn**, never `os.getpgid` — pids get reused.
4. **Drainers are load-bearing** (an unread pipe blocks the agent), and waiting on them is bounded by
   `DRAIN_GRACE_SECONDS` (a grandchild can hold stdout open forever).
5. **Persist a session id the moment it is disclosed.** Codex only reveals its `thread_id` mid-run;
   waiting until exit means a server that dies first loses any chance of resuming.
6. **Identity markers, not session ids, decide liveness.** On a fresh run codex never receives its id
   on the command line, so `store.process_alive` matches backend-supplied markers instead; status
   and the resume guards go through `store.record_process_alive`, which uses the recorded start
   time when one is present, so a process that renames itself after spawn (vibe) is still
   recognised. (A *resume* is the one exception — the thread id rides as a positional there, see
   `backends/codex.py` — but the marker-matching logic itself does not branch on that; it is
   unaffected.) See `tasks._identity_markers`.
7. **One live run per session**, checked against disk so two server processes cannot both resume it.
8. **`task_id` is validated before becoming a path** — it arrives from a caller.

## When the client restarts the server

Production MCP and ctl dispatch use detached task owners. Each owner executes the
prepared invocation, drains stdout/stderr, pumps stdin and publishes observed
settlement. MCP servers observe durable metadata/events and route input, approval
and control operations through their existing ledgers. Losing an observer does
not close the agent's pipes or launch another execution. See
[supervision and recovery](supervision.md) for the failure boundaries.

Caller authorization and lineage are resolved before the launch handoff. The
exec owner receives the prepared invocation over a private inherited pipe and
repeats final backend and saved-grant checks without rediscovering its caller.
A qualified launch reservation protects task/session identity before process
creation and ordinary metadata publication. An ambiguous launch stays reserved;
only a proven pre-execution failure permits an identical retry. Attached task
handles are read-only and cannot publish owner metadata.

The detached owner remains responsible through automatic continuation
reservation and successor acknowledgement. A workflow scheduler is a separate
detached owner; recovering that scheduler requires an explicit control action,
not a status or wait read. It reattaches to independently owned tasks and
reconciles durable decisions, reservations and linked children before scheduling.

Task-owner death still loses its pipes. Preserve all recorded output and process
identity, report capture loss or uncertain ownership, and refuse duplicate
execution. Do not infer exit zero from a missing exit receipt. Historical tasks
whose pipes belonged to an earlier in-process MCP server retain their existing
conservative recovery behavior.

`store.outcome_unobserved` identifies terminal metadata without an observed exit.
Only an unobserved `failed` may be overruled by the run's own terminal evidence;
`cancelled` always stands. Classification receives `exit_code: int | None`, never
a fabricated zero. Anything asking whether a task has settled uses
`resolve_status`, rather than the saved status alone. Cross-process signalling
retains the stricter identity/marker authorization in `identity.may_signal`.

**Live input — the rules that keep a message from being lost silently.** The pump (`task.pump`) is a
dedicated task, deliberately not in `task.watchers`, so `_finish_draining` neither waits on nor
cancels it; `_monitor` lets it finish its exit-path close (`_finish_pump`, bounded) before
publishing, so `undelivered` events precede `task_finished`. Every exit from the pump goes through
the one close protocol — flock `<id>.inbox.jsonl`, forward or report every queued message, create
`<id>.inbox.closed`, unlock, close stdin — and every send takes the same lock, so a message is either
accepted before the close (then written or reported `undelivered`) or refused after it. Nothing is
awaited while that lock is held. `send` answers "queued", never "delivered". Three edges, each from
review: the close re-checks its reason under the lock (an idle close whose idleness has passed is
abandoned; an error that arrived meanwhile drops instead of forwarding); the seal falls back to a
seal *line* in the inbox itself when the directory refuses the marker, and a close that can do
neither is retried `CLOSE_RETRY_LIMIT` times, then forced with a notice — a live run that can never
reach EOF would never settle — and a forced close still tries to seal, counts open background work
as abandoned, and leaves `inbox_reconciled` false unless it both read the disk inbox *and* the seal
landed (an unsealed inbox still looks open to other processes, which can append and be told
"queued" after that read), so `_finish_pump` does a final read and reports every unread line
`undelivered` before the run is published; and after exit, a lock that stays busy for `EXIT_CLOSE_GIVE_UP_SECONDS`
gets `_close_unlocked` (seal first, then read), which senders cover by re-checking the seal after
appending. A sealed inbox is not a settled task. `live_input` records the invocation capability;
`input_channel` reports current acceptance, and `input_closed_reason` explains a known closure
(the same reason appears in `supervision.live_input_closed_reason`). Once all owed results arrive,
stdin must close so the headless CLI can exit; autonomous follow-up work may still be running.
Wait for settlement before resuming. A send is also refused once the run's own process is confirmed dead, whatever its record
still says. A message written after a result persists `input_after_result` on the record, so a
recovered run whose replay shows no later result reads `failed`, not the earlier success.

Owner-local and cross-process messages both append to the durable inbox before
answering `queued`. A stalled process holding the inbox flock can still delay
idle closure; acquisition is nonblocking and cancellation remains available.
Delivery uncertainty is reported rather than replaying a message blindly.

**Queued follow-ups for tasks without live input (`followups.py`) are a separate ledger that never
touches the inbox, its `flock` or the close protocol above.** `send_message` on a standalone task
whose `live_input` is false appends to `<id>.followups.json` under `<id>.followups.lock` — an empty,
permanent lock file that retention leaves in place, like the other two. The state is one bounded
document replaced atomically under that lock (32 KiB per message, 16 outstanding, 960 KiB admission
budget inside a 1 MiB read budget; unreadable reads as `unavailable`, never guessed). It answers
`queued` with `delivery: "next_continuation"`, never "delivered". Consumption happens only through
explicit flags: `TaskRegistry.resume`/`resume_record` take `consume_followups` (set by MCP
`resume_task` and `harflow-ctl resume`) and `continue_task` takes it (MCP and ctl); the flag is
never inferred from `task_id`, which keeps meaning supervisor-owned for `pause_for_task`, and an
automatic continuation or a workflow resume never consumes. The sequence is reserve (sealing the
ledger, last step before the build), compose, build and spawn, then `consumed`; a failure is
`released` only when provably not started (`harflow_not_started`, or raised before `_spawn`),
`consumed` when the successor is registered or on disk, else `delivery_unknown`, which seals
permanently and is never re-queued. A terminal write that fails never changes the launch outcome:
`followups.finish` remembers it and the next lock acquisition in this process retries it first.
Read-time resolution (`reserved` with a successor record is `consumed`; with a dead owner or an
abandoned in-process attempt `delivery_unknown`; `queued` with a same-session successor
`undeliverable`) only repairs leftovers. Retention protects a task with queued or reserved items
(and a successor a reservation names) and holds the follow-up lock across re-reading the ledger,
deleting its state file and unlinking the record.

**Lock order, extended:** session lock (or the `continuation-<root>` chain lock), then the task's
`<id>.followups.lock`, never the reverse; the reaper takes its catalog locks and then the follow-up
lock, and never a session lock while holding it.

## Resource reservations (`resources.py`)

Named, exclusive, cross-process locks for dispatched work. The state is one private ledger,
`<state_root>/resources/ledger.json` (`harflow.resource-ledger` v1), so every server and `harflow-ctl`
process on the machine sees the same holders and queue. This module knows nothing about tasks or
workflows: an entry carries an owner (`task`, `workflow_run` or `workflow_attempt`, referenced by id
only; liveness is never stored) and each owner kind supplies its own "is it finished?" resolver.

- **The ledger lock is a leaf.** `flock(<state_root>/resources/.ledger.lock)` guards one bounded
  read, edit and atomic replace (temp file, fsync, `os.replace`, directory fsync). It is never held
  across an `await`, a subprocess (including the `ps` behind identity checks), Git, or the
  acquisition of another lock. Taking it while a session, chain or store lock is held is allowed;
  the reverse never happens, so it is last in the lock order: session or chain lock, then worktree usage lock, then
  `<id>.lock` and `<id>.followups.lock`, then workflow store locks, then the ledger lock. It is
  taken with a non-blocking retry and a timeout (`ResourceLedgerUnavailable`), so a stopped holder
  cannot freeze an event loop.
- **Generations.** Every entry has a `generation` bumped on each change. The reaper relies on it.
- **Grant rule.** A queued entry gets all its names at once or none, and only when every name has a
  free slot (`capacity(name)`, 1 today) and no earlier entry sharing a name is still queued: strict
  per-name FIFO, so a large request cannot starve and nothing is ever partially held.
- **Root declaration.** A requester running under a holding scope may only ask for names that scope
  declared; they are granted immediately as a borrow (no capacity, no queue). Anything else is
  `resource_not_declared_by_scope`, and a holding scope with no named reservation refuses every name.
  A waiter therefore never owns anything another waiter needs, which is what keeps wait-for cycles
  out. The caller resolves the scope; this module only enforces it. A parent released while
  borrowers are unresolved is kept, flagged `release_requested`, and removed with its last borrower.
- **Reaper.** `ResourceLedger.reap(resolvers)` reads under the lock, releases it, asks the resolver for
  each entry's owner kind (which may run `ps`), then re-takes the lock and removes only entries whose
  generation is unchanged and whose verdict was `settled` or `never_started`. A `keep` verdict, an
  unknown verdict, a raising resolver and an owner kind with no resolver all keep the entry:
  uncertainty never frees a resource. No resolver is registered until the owners that need one
  exist. Pollers write nothing when nothing changed.
- **Corruption.** A ledger that cannot be parsed or qualified makes every operation raise
  `ResourceLedgerCorrupt`; it is never silently reset. `harflow-ctl resources-repair` (humans only,
  the takeover gate) is atomic and fail-closed: `begin_repair` keeps the corrupt file as
  `ledger.corrupt-<timestamp>.json` and in the same step replaces it with a ledger carrying a repair
  marker that every operation refuses (`ResourceLedgerRepairing`); `task_resources.collect_evidence`
  then reads durable launch receipts, task records and workflow resource evidence outside the lock, and `complete_repair`
  publishes the whole reconstruction in one atomic replace. A crash in between leaves the ledger
  closed, not empty, and running the command again resumes it. The reconstruction keeps lending
  relationships: a lender stays (flagged `release_requested` once settled) while any borrower is
  unresolved, and a lender that cannot be found is replaced by a conservative entry owned by no task
  that blocks its borrowers' names until they finish or a person releases it; a lender that lost a
  name conflict to another restored holder is replaced the same way, so a borrower never holds
  fewer names than it used. It refuses a healthy
  ledger. Waiting launches are not restored.
- **Tasks use it through the launch receipt.** A task that asked for resources is acknowledged while its
  detached owner waits (`task_supervisor._acquire_resources`): the receipt moves
  `claimed -> waiting_for_resources -> granted -> spawned -> settled`, with `not_started` reachable
  from every state before `spawned` (from `granted` only when a failure is proven to precede the
  spawn). No TaskRecord exists until spawn, so `record.pid` and `outcome_unobserved` readers never
  see a pending task; `task_resources.PendingLaunch` and `view` are built from the receipt, and
  `launch()` returns a `PendingLaunch` where it used to return a task. The whole pre-spawn
  qualification in `_execute_prepared` runs *after* the grant. The reservation id is written to the
  receipt before the entry can exist. The owner releases on `_monitor`'s terminal-publish path only
  when the exit was observed (never on the `abandoned`/`CancelledError` path, invariant 1); the
  reaper covers everything else.
- **The task resolver** (`task_resources.task_resolver`) decides from durable state by task id:
  a conclusively settled record (`retention._conclusively_settled`) is `settled`; a launch
  `not_started`, or `waiting_for_resources` with a dead owner (spawn requires `granted` first), is
  `never_started`; everything else, including `granted` with no record and an owner that cannot be
  inspected, is `keep`. Waiting launches also run the reaper (every 15 s), as does the server's
  maintenance pass.
- **Sessions and follow-ups.** A waiting or granted launch is unresolved, so its session stays busy;
  `session_reservations` releases only a `waiting_for_resources` launch whose owner is conclusively
  dead. Queued follow-ups handed to a waiting launch are re-owned by the launch owner
  (`followups.handoff`); `followups._launch_verdict` reads the successor's receipt (in progress,
  released to queued on `not_started`, `delivery_unknown` for `granted` with a dead owner and no
  record) and the owner finalizes consumption at spawn. A continuation's reservation file gains
  `accepted`, distinct from `started`.
- **Scope.** `task_resources.holding_scope`: a detected caller whose record holds a reservation lends
  it; a caller with a `workflow-owners/<task_id>` receipt is a holding scope with no reservation and
  refuses every name (stage D widens this); other callers are no scope. A continuation stays under
  its source's lender while that lender still holds.
- **Worktrees (`worktrees.py`).** A task's `worktree` is *planned* by the parent
  (`TaskRegistry._plan_worktree`: supervised only, never `read_only`, never for a detected or
  undetermined caller) at the deterministic path `<root>/<task_id>`, so the invocation, markers and
  `request_sha256` bind to it; the receipt carries `worktree {state: planned -> creating -> created
  | failed}`. The *owner* creates it (`task_supervisor._create_worktree`) after any resource grant,
  acknowledging first with `granted` so a long checkout cannot outlast the caller's handshake, then
  rebuilds the invocation against the real tree (`TaskRegistry._rebuild_invocation`) before the
  unchanged `_execute_prepared` pre-spawn block. A crash in `creating` is reported as `unknown`;
  nothing is removed automatically, and an owner that died before `created` never spawned, so its
  launch releases its session and resources. `launch()` and `worktree-remove` serialize on the
  per-worktree usage lock, which `launch()` takes whenever the worktree is named by the recorded plan or the
  resumed record's `worktree` (never inferred from the current `HARFLOW_WORKTREE_ROOT`) and which is keyed by
  the canonical path; `worktree-remove` runs no Git command inside the tree until `worktrees.verify_link`
  has proven its `.git` file leads to `<common>/worktrees/<name>` of the recorded source; the lock sits with the other file locks (it is never held while waiting
  on the ledger). `record.repo_path` is the worktree; retention keeps a task whose worktree exists.
- **Recovery commands** are ctl-only and use `lineage.detect_caller_detail` and fail closed like
  takeover: `resources-release` removes one reservation after `--confirm-uncertain` (a reservation
  with borrowers is flagged and goes with its last borrower), `launch-settle` settles a recordless
  `granted` launch whose owner is confirmed dead (reservations, session claim, worktree usage and
  follow-ups, from the launch receipt alone), and `resources` is the ungated read the Monitor uses.
  Every verdict that frees something after an owner-liveness probe re-reads the receipt and record
  afterwards (`task_resources.owner_confirmed_gone`): a probe can be slow, and an owner that granted,
  spawned and died meanwhile left an agent.

## Shared scratch (`shared_scratch.py`)

Detail behind [shared scratch](shared-scratch.md); the rules that keep it safe:

- **Two namespaces, one root.** Per-task scratch stays `<root>/scratch/<task_id>`. Shared folders live under
  `<root>/shared-scratch/{chain,group}/<key>`, separate because `TASK_ID_PATTERN` accepts the ids `chain` and
  `group`, which would collide under `scratch/`. A chain key is a validated task id (`scratch_chain_id`, the first
  task's id, inherited by resume and continuation; `chain_root_task_id` is untouched). A group key is a slug plus 16
  hex digits of the SHA-256 of the exact string. Bridge-owned metadata (`.meta/<kind>/<key>.json`, `last_used_at`)
  sits outside the agent-writable folder and is verified on reopen.
- **Allocation happens in the launch owner**, in `_execute_prepared`, after the task scratch and before the extra-path
  and bridge-state checks, so all four scope checks see the shared folders. The per-folder lock is taken (chain, then
  group) with a bounded non-blocking poll before the pre-spawn window, held in a `Hold`, and released as soon as the
  task record is durable (`persist`), and always on the way out of `_execute_prepared`. It is a leaf lock: never held
  across a subprocess, a Git call or another lock, ordered after the session or chain lock and before `<id>.lock`.
  Metadata updates take the same lock, so they cannot reorder; callers that already hold `<id>.lock` (retention, the
  settlement hook) only try it for a bounded time and keep their record on failure. Cleaners unlink the lock file
  under the lock and waiters re-verify the file's identity after `flock`.
- **A reference is anything that may still use the folder**: an unsettled or uncertain record, an unresolved or
  pending launch receipt (including one waiting for resources and a reserved continuation), a pinned record (workflow
  or queued follow-ups), an unreadable record, or a retained record with a registered artifact under it. Worktree
  tasks are ordinary: they get shared scratch like any task, and their receipts count as references like any other.
- **Cleanup renames under the lock and deletes outside it** (`.trash`), never following symlinks. The age policy
  runs from the retention sweep, after `_sweep_native_events`, and counts from the newest of the folder's
  `last_used_at` and its records' observed `finished_at`; `retention._delete_task_files` first carries a record's last
  activity into the metadata (and keeps the record if it cannot) so shorter task retention cannot shorten it. A task
  whose exit was never observed has no finish time: the sweep records the moment it found it settled
  (`observe_settlement`) before counting age. One snapshot selects candidates and each candidate is rechecked under its
  lock from its own members only. The sweep runs even when task-record retention is disabled.
- **Nested grants never widen.** `TaskRegistry._caller_allows_shared` grants a confined dispatcher's child a shared
  folder only if it is already one of the dispatcher's writable roots or a chain it began (strict `spawned_by`
  ancestry); everything unreadable denies.
- **Guidance goes through measured system-prompt seams, never the prompt.** `Capabilities.system_guidance`
  (`first_turn_sticky`, `per_run` or none) and `with_system_guidance` live in `backends/`; nothing outside them
  branches on a backend's name. Each `assert_safe` accepts exactly the one static note (`system_guidance.TEXT`), once,
  in exactly one position, so no caller string can take the seam. It is applied after the approval configuration and
  re-applied when a worktree launch rebuilds its invocation, never blocks a launch, and is disclosed per run in
  `scratch.system_guidance`; `reused_from_session` is claimed only when the resumed task itself carried the note.
- **Access states are claims about what Harflow granted** (`writable`, `no_additional_grant`, `sandbox_read_only`),
  derived from the real grant and the recorded `Enforcement`, never from the freedom's name. Caller approvals can run a
  command outside the sandbox, so a Codex run with them is never reported `sandbox_read_only`.

## Takeover, ctl control commands, open-app (A4)

- **Takeover is humans-only and ctl-only.** Never expose it over MCP. `ctl takeover` and
  `takeover-attach` refuse when `HARFLOW_TASK_ID`, a worker depth, or their legacy predecessors are set at all or a caller is detected: the interactive
  session runs under the user's own permissions, so an agent reaching it escapes its enforcement.
  The gate **fails closed**: it uses `lineage.detect_caller_detail`, which tells "positively no
  caller" from "could not look" (`ps` denied, unreadable ancestry/session, a related task that is
  neither confirmed alive nor dead), and refuses the latter as `caller_undecidable`. Plain
  `detect_caller` reads both as None, which is right for lineage and wrong for a gate.
- **A takeover's `.ready` is terminal for lease purposes.** Its controller (`harflow-ctl`) exits
  right after writing it, so `control.recover_abandoned` — which reads anything without `.sig`/
  `.failed` as pending — must never be pointed at the takeover family; `begin_takeover` has its own
  recovery.
- **The session is busy from `.req`**, via `store.live_session_ids` (records ∪
  `control.takeover_reservations`), until `.failed`, the 120 s window lapsing with no attach (a
  pending attempt also stays busy while its controller is not provably abandoned — a deliberate,
  test-pinned divergence from the plan, so a slow cascade cannot let a resume in before `.ready`),
  or the attached
  process confirmed `dead`. `undecidable` is busy. Retention keeps a task exactly while it is busy.
- **Lock order is session lock, then `<id>.lock`**, everywhere (`takeover-attach` takes both). The
  follow-up lock (`<id>.followups.lock`) comes after the session or chain lock and never before it; see
  the queued follow-ups paragraph above.
- The interactive commands were checked against each CLI's `--help` only (2026-09-25,
  `tests/test_interactive_resume_real.py`); each backend's `interactive_resume_argv` docstring
  records what was measured. Never run them from a test: they open a TUI, and codex's writes a trust entry to
  `~/.codex/config.toml`.
- **`ctl run`/`resume` fork.** The child redirects stdio before anything else (a reader of the
  parent's stdout must see EOF when the parent exits) and clears the fork-copied
  `identity.own_identity` cache. Test the fork from a fresh interpreter (`tests/ctl_driver.py`), not
  from pytest's own process, which carries threads by then.
- **Automatic app opening is currently disabled.** `tasks._maybe_open_monitor` returns
  immediately until the companion app's managed runtime integration lands, so
  `HARFLOW_OPEN_MONITOR=1` opens nothing (`docs/development.md` says so). The variable still matters:
  extra writable path grants are refused while it is `1` (`runtime_paths.py`), and conftest sets it
  to `0` as a belt-and-braces default. When opening is re-enabled, keep the predecessor's rules: it
  never changes an outcome, is scheduled synchronously after registration, applies to root tasks
  only, uses an injectable launcher, turns every exception into a notice, and stays opt-in.

- ctl itself never starts retention and never starts the notification worker: `workflow inspect` /
  `assigned-input` resolve managed authority and share the server's checks through
  `workflow_node_reads` without importing `server` (or its MCP stack), and the ctl paths that do
  need registry-backed helpers (`permission-preview`, mutating workflow actions) install one with
  `server.install_cli_registry`, which starts neither. `run`/`resume` still build their own
  registry deliberately — see `retention.py` for why the sweep is a live-server job.

## Enforcement must never overclaim

This is the point of the abstraction, and the easiest thing to get subtly wrong. Every boolean in
`Enforcement` is a strict claim: True only if the named thing genuinely **cannot** happen. A caveat
does not repair a boolean that says something untrue.

Worked example: Claude's deny patterns refuse `git commit` but are evaded by `git -C` and
`bash -c` (measured). So `commit_push_blocked` is **False**, and the weaker truth lives in
`direct_commit_commands_denied: True`. Likewise Codex confines writes by OS but to workspace *plus*
temp dirs, so `writes_confined: True` is paired with `writable_roots` naming them — never
"confined to the repo".

**`freedom` is a requested ordering, not four distinct strengths everywhere.** Backends may collapse
adjacent levels, and two do: on opencode `publish` is byte-identical to `write_in_repo` (nothing was
ever enforced there), and on vibe it is byte-identical to `unrestricted` (only `auto-approve` can
publish). Where two levels produce the same argv, `assert_safe(argv, freedom)` genuinely cannot
refuse a mismatched freedom — there is no difference to detect — so those pairs are listed
explicitly, excluded from the cross-freedom refusal test, and covered by a test asserting they
really are identical. A collapse that is pinned by a test is a documented property; one that is
merely true is a hole waiting to be mistaken for enforcement.

The publication authorization field is `publish_attempts_allowed`. It reports only Harflow's
requested authorization, not whether credentials, hooks, remote permissions, branch protection or
the agent permit publication. The previous spelling remains a deprecated output/read alias for
this release; contradictory aliases are refused. `network_access` separately reports network
controls; `not_controlled` means the surrounding environment decides.

If you add a backend or a freedom level, re-measure rather than reasoning about it, and record what
you measured in [backend-cli-facts.md](backend-cli-facts.md). `tests/test_backends.py::test_enforcement_never_overclaims` enforces the shape;
it cannot check whether your claim is true.

## Telling the caller what happened

The caller is usually a model, and it only knows what the tool surface says. State changes it cannot
infer belong **in the payload**: `enforcement` on every task, `recovered: true` plus a `note` on
tasks from an earlier process, `next_step` when a wait returns still-running, `notices` for non-fatal
messages, and errors that say what to do instead of just what failed.

**MCP status responses are shaped in `server.py` (`_status_payload`), never in the snapshot.**
`get_task_status`, `wait_for_task` and `cancel_task` carry `recent_activity` (≤ 5 one-liners from the
normalized event log) instead of the ~10k-token raw `last_output_tail`, which comes back only with
`include_tail=True` — or shortened, on a failed run, where the raw lines are what diagnoses it.
`get_task_events` pages the normalized log on demand. `Task.snapshot()`/`store.snapshot()` keep the
full tail because `harflow-ctl --json` is a public wire contract (`harflow.ctl` v1, see `docs/contracts.md`); reshape the MCP
layer, not the snapshot.

A dispatch at `publish` or `unrestricted` also checks whether the checkout is on the repository's
default branch, and says so on `bridge_notices` — a channel separate from `Accumulator.notices`
precisely because vibe's `ingest` resets those per turn and would discard a dispatch-level notice on
a resumed run. Three rules keep that honest:

- It is **disclosure, not a guard.** `start_task` returns after the process has spawned, so the
  notice cannot gate anything. It describes the checkout observed at dispatch; the agent may
  switch branches later. It does not predict or establish a commit or push on that branch.
- The default branch is only ever what a remote's own locally recorded `HEAD` says. **Never fall
  back to "the branch is named `main` or `master`"** — a feature branch can be called `main` while
  the real default is something else, which is a test. No fetch, no `ls-remote`, no
  `remote set-head`.
- **Not being able to tell is reported too.** For a run authorized to publish, "the checkout at dispatch could not be determined" is material, so silence would be the wrong answer. Every
  failure mode — missing `git`, a timeout, a detached HEAD, several remotes with no `origin` — is
  absorbed into that notice rather than escaping, per the invariant that bookkeeping must never
  change an outcome. The probes run off the event loop, since they are blocking calls inside an
  async `_spawn`.

`wait_for_task`'s default (55s) and the other waits' defaults stay short because a client's request
deadline is not discoverable from the server (see [backend-cli-facts.md](backend-cli-facts.md)); an explicit
`timeout_seconds` up to 3600, or `until="terminal"` (6 hour ceiling), is honoured as asked, never
clamped. A request that outlives the client's deadline fails the *call* while the run continues
unharmed, so every wait is resumable by calling it again. The measured deadlines are under "MCP
client request deadlines" in [backend-cli-facts.md](backend-cli-facts.md); waits are read-only and never write, signal or
resume (`wait_policy.py` is the shared contract).


### Workflow resource owners

`workflow_resources.py` binds run and candidate-attempt ownership to the ledger. `_dispatch`
persists `resource_attempt {id, candidate_index, state, reservation_id}` before enqueueing, acquires
names before a tree slot, and atomically attaches the grant to `dispatch_reserved`. Fallbacks use
new owner IDs. Publication failures and slot-wait cancellation release pre-dispatch grants;
uncertain dispatches keep them. Reconciliation of a dead scheduler marks queued or unattached
granted attempts not started without treating a resource-only activation as a failed execution.
Read-only resolvers consult persisted run/attempt state; they never reconcile. A run is releasable
only after terminal settlement and resolution of its linked descendants and attempts, regardless
of supervisor liveness. Borrowers defer release through the ledger.

Repair combines task and workflow evidence before reconstructing lenders. Granted and attached
attempts and run-scope holds survive reconstruction; settled lenders are restored only when an
unresolved borrower still needs them. A workflow publishes its grant and confirms it against the
ledger before proceeding, retrying acquisition if an earlier repair omitted queued evidence.
An unreadable run leaves repair closed. Waiting holds no workflow, usage, session or ledger lock.
Named holders may wait on tree slots or bounded checkout leases, disclosed in status.

Workflow worktrees use the existing hardened creation and removal implementation. The root run's
plan is durable under the usage lock before creation. The supervisor creates it after the run
resource grant, then publishes its execution `repo_path`; linked children inherit that path and
source metadata. Run records participate in inventory and removal user checks between tasks.
Shared scratch allocation and measured system guidance remain owned by the task launch path.

Workflow holding-caller launches establish a durable workflow-run borrow anchor before supervisor
launch or return. Runtime-only implicit anchor names never change saved definitions. Ledger
borrowing flattens to the ultimate lender, and persisted repair evidence keeps that actual link
and the lender's complete declared coverage. Run publication under a worktree usage lock occurs
on a worker thread carrying state context, without holding the lock across an await. Dispatch
exits settle tree permits before individually guarded resource bookkeeping; cleanup cannot replace
an observed outcome. Run-level resource waits and activation waits are persisted separately.

Workflow root creation centralizes usage locking, durable lookup registration and run publication.
Ordinary task/workflow containment reads only bounded workflow-worktree receipts, never the full
workflow listing catalog. Receipts use bounded nofollow/source-stable reads and selected-root
qualification; conflicting run identities cannot overwrite them. Recovery ensures registration
before Git creation. Human inventory and removal retain full qualified workflow settlement checks.

Workflow-worktree receipt registration canonicalizes paths once. Decoding qualifies normalized
absolute stored paths lexically, without resolving the current agent-controlled checkout or source
filesystem. Ordinary containment canonicalizes returned paths for comparison; filesystem changes
cannot turn an unchanged lookup receipt into incompatible metadata. Removal still proves current
workflow settlement and Git linkage independently.

Workflow publication never enumerates full task metadata to locate a checkout lock. It uses small
launch/lookup receipts, then qualified direct records for candidate task IDs in lexical and canonical
requested-path ancestors. This fallback retains the usage-lock guarantee when partial retention has
removed a launch before its task metadata. An external resource_scope is provenance only: absent an
own held grant, root coverage is empty. Linked children refresh the parent chain before saved scope
hints, while root grant acquisition explicitly selects its original lender.

Lookup receipts remain after removal; no automatic pruning or repair is part of G04. Corrupt
engine-owned lookup metadata refuses lookup. Slot-wait disclosure observations and writes are
best effort; they cannot replace the tree's actual liveness checks or alter dispatch outcomes.
