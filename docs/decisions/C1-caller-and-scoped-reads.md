# C1: caller identification and scoped-read rules

Status: specification of Python 0.0.11 behaviour (tag `v0.0.11`, contract manifest `cd58a5ef…`), written
so the Rust engine can implement the same rules from this text. It describes what Python does today.
It does not change any contract, and it does not decide the open points listed in "Unknowns and
candidate deltas". `docs/contracts.md` stays the public wire specification; where this document and
the contracts overlap, the contracts win and the difference is recorded as a question.

Evidence: the code paths cited below, plus the E1 scenarios in `conformance/e1`, which pass against
Python 0.0.11. Each rule is marked:

- **[V]** verified by an E1 scenario against Python 0.0.11 (case named).
- **[R]** read from the Python source and not exercised by a scenario.
- **[U]** unknown or undecided; see the last section. Nothing is guessed.

## 1. Terms

- **Task process.** The OS process a task record names through `pid`, with `start_time` and
  `markers` as its identity (section 3).
- **Caller.** The task whose process, or a descendant of it, is running the current ctl command.
- **Direct caller.** No task caller was found and the absence was positively established.
- **Agent caller.** A confirmed task caller, or a worker environment marker (section 2). Includes
  standalone tasks and workflow-associated tasks.
- **Managed caller.** An agent caller that a workflow association scopes: role `builder`, or any role
  in a run whose `execution_contract` is `delegation` (section 6). Managed callers get a restricted
  view of task reads; every other agent caller does not.
- **Undecidable.** A caller could not be established as present or absent. Always fails closed.
- **Authority-gated read.** A read that consults the caller before answering (section 5 lists them).

All state is under the selected root; every check below runs after the root has been qualified
(the ordinary-read initialization behaviour: a missing or empty root is initialized by any ordinary
read, a foreign or incompatible one refuses with `state_incompatible` and is left untouched).
**[V]** `reads.missing_root`, `list.missing_root_initializes`, `list.foreign_root_refused`.

## 2. Environment signals

`harflow_mcp.takeover.env_agent_refusal` (the cheap check, no state access): the caller is an agent
when **any** of the keys `HARFLOW_TASK_ID`, `PB_TASK_ID`, `HARFLOW_DEPTH`, `PB_DEPTH` is present in the
environment. Presence is the whole test: empty values, malformed depths and ids of unknown tasks
count. The refusal is `agent_caller` with a message that names the variables.
**[V]** `scope.environment_markers` (six marker combinations on all six direct-only reads).

Only `HARFLOW_TASK_ID` (not `PB_TASK_ID`, not the depth variables) influences the managed-reader path
(section 4): when it is set and detection finds no confirmed caller, the read fails with
`workflow_read_refused` and the message "Workflow caller task identity cannot be verified". A set
`PB_TASK_ID` or depth variable alone does not stop `status` or `list`.
**[V]** `scope.unverifiable_task_identity`.

Mutation callers use the stricter `caller_policy.signals`: values are validated (task id charset,
legacy id must equal the current id, depths ASCII decimal) and a present signal requires a live,
related, confirmed parent whose id equals `HARFLOW_TASK_ID`. This is outside the read scope and is
recorded here only so the two uses of the markers are not confused. **[R]**

## 3. Process identity

Used to confirm that a task record still denotes a live process (`identity.py`).

- Lookup: `ps -o lstart=,command= -p PID` with `LC_ALL=C`, bounded to 5 s. Rust may use another
  mechanism, but must produce the same verdicts for the same facts.
- `start_time` is the `lstart` text with runs of whitespace collapsed to single spaces.
- Verdict `alive`: the process exists, `start_time` equals the recorded value, and every recorded
  marker is a substring of the command line (no markers means a start-time match suffices). An
  alternative is accepted when the trimmed command line equals one of the backend's declared
  `caller_process_titles` (a backend that renames its process after spawn).
- Verdict `dead`: `ps` confirms the pid is absent (nonzero exit with empty stdout and empty stderr),
  or the start time differs (the pid was reused).
- Verdict `undecidable`: `ps` failed, produced unusable output, or the start time matched but markers
  are missing and no title matched. A record with no `start_time` (legacy) is `undecidable`
  (markers seen) or `dead` (markers not seen); with no markers `undecidable`.
- A pid that is not a positive integer is `undecidable`.
- Only `alive` confirms a caller. `undecidable` must never be read as either present or absent.

**[V]** indirectly: `scope.agent_caller_by_ancestry` registers the test process with a real
`start_time` and no markers. The `dead`, `undecidable` and marker branches are **[R]**.

## 4. Caller detection

### 4.1 Candidates

A task record is a candidate caller unless it is observed-terminal, meaning `status` in
`completed`, `failed`, `timed_out`, `cancelled` **and** `exit_code` is not null. A terminal record with
no exit code (an unobserved outcome) stays a candidate. The scan must be complete: any refused or
unreadable task metadata makes the whole candidate read fail (section 7). One retry of the whole scan
is allowed only when every refusal positively identifies a source that changed during its read.
**[V]** `scope.incompatible_metadata_fails_closed`; retry **[R]**.

### 4.2 Methods and precedence

`lineage.detect_caller` tries, in this order, and stops at the first confirmed candidate:

1. **Environment id.** `HARFLOW_TASK_ID` matching `[A-Za-z0-9][A-Za-z0-9_-]{0,63}` and naming a
   candidate that is confirmed `alive` **and** related to this process by session or ancestry. An
   unrelated live record named by the variable is never trusted. Method name `pb_task_id`.
2. **Session.** Candidates whose `pgid` equals this process's session id (`getsid(0)`) and which are
   confirmed `alive`; the one with the greatest `started_at` wins. Method name `session`.
3. **Ancestry.** Walk this process's parent chain (section 4.3); for each ancestor pid in order from
   the nearest, take candidates whose recorded `pid` equals it, newest record first, and return the
   first confirmed `alive`. Several records can share a pid after reuse, so every one is checked.
   Method name `ancestry`.

Any exception during detection yields "no caller" in `detect_caller` but "undecidable" in
`detect_caller_detail` (4.4). State-ownership failures propagate in both.
**[V]** ancestry, `scope.agent_caller_by_ancestry`. Session and environment-id methods **[R]**:
the E1 harness cannot place the engine in a task's session, and the environment-id method needs a
related record, which the harness's ancestry also provides (the ancestry caller wins and the
environment value is then irrelevant; `scope.unverifiable_task_identity` pins that precedence).

### 4.3 Process table and ancestry

A system-wide `ps -axo pid=,ppid=` snapshot is cached for 5 s per process and refreshed after a fork.
`ancestors(pid)` returns the parent chain excluding `pid`, stopping at parent 0 or 1, at a missing
entry or at a cycle (the stopping pid is not included). A missing or unreadable table, or this
process missing from it, is a failure to look (undecidable for gates). Rust must keep these
stopping rules; it may read the table differently provided the result is the same. **[R]**

### 4.4 Gate detection (fail closed)

`lineage.detect_caller_detail` returns a confirmed caller, or `Detection(None, None)` only when the
negative was established, or `Detection(None, reason)` (undecidable). After `detect_caller` finds no
caller it additionally requires: the process table was readable; this process is in it; the session
id was readable; and **no candidate related to this process** (`pgid == sid`, or `pid` in the ancestor
chain) has a verdict other than `dead`. A related candidate that is `undecidable` makes the whole
detection undecidable ("task X is related to this process and could not be confirmed alive or
gone"). Candidates are the single scan from 4.1, not re-read after `ps`. **[R]**

### 4.5 Catalog detection (bounded APIs)

`lineage.detect_catalog_caller` serves reads built on the bounded task catalog (`task-list-page`
and its relatives). It never falls back to a history scan. It queries the indexed caller table for
non-observed-terminal rows where `pgid = sid`, or `pid` in the first 100 ancestors, or `id` equals
`HARFLOW_TASK_ID`, at most 101 rows. More than 100 rows is undecidable. It then applies the three
methods of 4.2 to the decoded records. With no confirmed caller: a set `HARFLOW_TASK_ID` is
undecidable ("Workflow caller task identity cannot be verified"); otherwise any related candidate not
confirmed `dead` is undecidable. An incomplete catalog is undecidable ("Task catalog bootstrap is
incomplete") and surfaces as the preparing/blocked page of section 8. **[R]** (page behaviour **[V]**
`task-list-page.paging`, `scope.incompatible_metadata_fails_closed`).

## 5. Gates by command

"Direct-only" reads call `takeover.caller_refusal(log_dir)`: first `env_agent_refusal` (section 2),
then `detect_caller_detail` (4.4). Result: confirmed caller → `agent_caller` with the message
"it was called from task ID (detected by METHOD)"; undecidable → `caller_undecidable` with the reason;
otherwise allowed. Error precedence for these reads is **usage, then authority, then task, then
cursor**; authority runs before the task id is resolved, so a refused caller cannot learn whether an
id exists. **[V]** `scope.agent_caller_by_ancestry` asserts known and unknown ids refuse byte-for-byte
identically.

| Command | Gate | Refusal code | Notes |
| --- | --- | --- | --- |
| `task-events`, `task-prompt`, `task-changes` | direct-only | `agent_caller`, `caller_undecidable` | message prefix "Task history reads require a verified direct caller: " |
| `statistics-page` | direct-only | `agent_caller`, `caller_undecidable` | prefix "Statistics require a verified direct caller: " |
| `task-list-page --predecessor-task-id` | direct-only | `agent_caller`, `caller_undecidable` | prefix "Successor discovery requires a verified direct caller: " |
| `interaction-events` | direct-only | `unauthorized` for **every** refusal | one fixed message "Interaction events require a direct caller"; no `agent_caller` or `caller_undecidable` distinction **[U1]** |
| `limit-hits`, `task-list-watch` | direct-only | `agent_caller`, `caller_undecidable` | not covered by E1 **[R]** |
| `workflow-notifications` | direct-only | `notification_error` for every refusal | not covered by E1 **[R]** |
| `mcp-allowlist` edits | direct-only | `human_only` | not covered by E1 **[R]** |
| `status`, `status-batch`, `list`, `recovery-activity` | managed reader (section 6) | `workflow_read_refused` | open to every non-managed agent caller **[V]** |
| `task-list-page` (without the predecessor option) | managed page reader (section 6) | none; withholds items | open to non-managed agent callers **[V]** |
| `task-artifacts` | artifact caller (section 9) | `artifact_error` for every refusal | a task caller reads only its own |

`status`, `list` and the page read are therefore open to a standalone agent caller over **all** tasks.
Only history-class reads (events, prompt, changes, statistics, interaction feed, successors) are refused
to agents. **[V]** `scope.agent_caller_by_ancestry`. Whether that breadth is intended is **[U2]**.

The exit status of a refusal is 1; usage errors are 2 and precede authority. **[V]**

## 6. Managed callers

`workflow_inspection.managed_reader` (used by `status`, `status-batch`, `list`, `recovery-activity`):

1. Detect the caller with `detect_caller`; if none, run `detect_caller_detail`. An undecidable result
   raises "Workflow caller authority is undecidable: REASON"; no caller and a set `HARFLOW_TASK_ID`
   raises "Workflow caller task identity cannot be verified"; no caller otherwise returns no
   management (direct view). Both raises surface as `workflow_read_refused`. **[V]**
2. With a caller: look up its workflow association strictly (receipt file
   `workflow-owners/TASK_ID.json`, then inherited ownership through `parent_task_id`/`spawned_by`
   ancestors, at most 64 visited, cycle or overrun is an error). No association returns no management
   (direct view): a plain standalone agent is **not** scoped. **[V]** (standalone case)
3. An association scopes the view only when its role is `builder`, or the run's `execution_contract`
   is `delegation`. Workers of other runs are not scoped by this reader. **[R]** **[U3]**

A managed view restricts task reads (`guard_task_read`, `allowed_task_ids`, `filter_task_reads`):

- A builder reads only its own builder tasks of its own run.
- A worker node reads only its own execution.
- An orchestrator reads the tasks of its own run's activations.
- Any other task is refused with "Managed agents may only read tasks from their own workflow run"
  (`workflow_read_refused`), identically for existing and nonexistent ids.
- `list` and the page read filter rows to the allowed ids; the page also drops `next_cursor`
  (forces `has_more: false`) and recomputes `total_active_count` from the filtered rows, removing the
  root-level totals.
- Id resolution for a managed caller is closed: it resolves prefixes only within the allowed ids; an
  exact or canonical-UUID value outside them passes through unchanged so the guard answers the same
  as for a missing id, and an unauthorized id is never counted as an ambiguity candidate (the
  contract's "Id prefixes" section).

None of section 6 beyond item 1 and the standalone case of item 2 is covered by E1: it needs workflow
run documents and receipts. **[R]** Workflow-managed scenarios belong with E6.

## 7. Incomplete or incompatible history

Task metadata that is foreign, incompatible or unreadable anywhere in the root makes caller
detection fail (the scan must be complete). The visible result is uniform: **every authority-gated
read fails closed, whether or not it involves the damaged task.**

| Read | Result | Code |
| --- | --- | --- |
| `list`, `status`, `status-batch`, `recovery-activity` | refused | `workflow_read_refused` ("Workflow caller authority is undecidable: caller detection failed: …") |
| direct-only reads (not `interaction-events`) | refused | `caller_undecidable` |
| `interaction-events` | refused | `unauthorized` |
| `task-artifacts` | refused | `artifact_error` ("Task catalog caller identity is unavailable") |
| `task-list-page` | success, nothing withheld except content: `items: []`, `related_headers: []`, `next_cursor: null`, `has_more: false`, `counts_complete: false`, `authority_incomplete: true`, `history_incomplete: true`, `note`, and `catalog_state` limited to `status` (`blocked`), `source`, `reason` | none |

**[V]** `scope.incompatible_metadata_fails_closed`. The detection error text can name the damaged
task id; the scenarios do not pin the message. This contradicts the contract sentence that lists
"may display compatible unrelated tasks while reporting `history_incomplete`" for ctl `list` **[U4]**.

## 8. Cold index and preparation

The first bounded read on a root builds a derived listing index. While it is not ready, the page read
returns the shared preparation response (contract "Bounded read authority preparation"): `items: []`,
`related_headers: []`, `next_cursor: null`, `has_more: false`, `counts_complete: false`,
`bootstrap_pending: true`, `catalog_state` holding only `status`, `source`, `reason`; no counts, no
blocker types and no other catalog field, even for a caller later shown to be direct. Clients re-issue
the read. **[V]** (the scenarios retry up to 40 times with 250 ms spacing and check this shape whenever
it appears; Python showed it on the first call of a cold root). An engine with no derived index may
never produce it; the scenarios then simply never see it. `statistics-page` has the analogous
`state: "preparing"`. **[V]**

## 9. Artifact caller

`artifacts.get` is its own caller check (not the managed reader):

- The caller is established independently of section 4.2, by `artifacts._caller`, and rejects
  ambiguity and spoofed hints instead of choosing:
  1. Read the process table and this process's ancestors and session (unavailable → "Caller ancestry
     is unavailable").
  2. Take `lineage.related_catalog_candidates`: non-observed-terminal catalog rows with `pgid = sid` or
     `pid` in the ancestor chain; the chain is capped at 100 and the rows at 100 (more is an error).
  3. Check each related row's identity: a verdict other than `alive` or `dead` raises "Caller identity
     is unavailable"; `alive` rows are the matches.
  4. If any match is an ancestor, the nearest ancestor owner wins, but a session-only rival among the
     matches raises "Caller task identity is ambiguous".
  5. If `HARFLOW_TASK_ID` is set, there must be exactly one match and it must be that task, otherwise
     "Caller task identity is ambiguous or unverifiable". More than one match is the same error.
  6. No match and `HARFLOW_ROOT_TASK_ID` set raises "Caller task identity is unverifiable"; no match
     otherwise means a direct caller.
  7. The selected record is re-read and must still be non-terminal-observed, with unchanged pid, pgid,
     start time and markers and an `alive` verdict, else "Caller task identity changed or is
     unavailable". Then `_authority` applies (below).
  The depth variables and `PB_TASK_ID` play no part here. **[V]** for the ancestry caller and the
  unverifiable `HARFLOW_TASK_ID` (`scope.unverifiable_task_identity`); the other branches **[R]**.
- A task caller may name only itself: another id raises "Task callers may read only their own
  artifacts". A prefix resolves inside the singleton set `{caller}`. A direct caller resolves against
  the catalog, and a missing exact or short id passes through to the "Unknown task" answer; only
  ambiguity and real prefix failures are reported as the resolution codes (contract "Id prefixes").
  **[V]**
- Authority over the task's own workflow binding (`_authority`) refuses builders and managed ancestry
  mismatches; this is the managed-artifact rule and is **[R]**.
- Every refusal is `artifact_error` with exit 1, including an unknown task, bad limit (1..50), bad
  cursor and unavailable authority. **[V]** Whether to distinguish authority from usage is **[U5]**.
- A readable manifest with a foreign header is not an error: `state: "unavailable"`, empty items.
  **[V]**

## 10. Id resolution and authority

Resolution order for task ids in authority-gated reads (also the contract's "Id prefixes"):

1. Validate the id charset first (`invalid_task_id`, exit 1).
2. Authority first: a managed caller resolves only inside its allowed ids; a direct caller (and any
   non-managed agent caller) resolves against the root catalog and pending launch receipts.
3. Exact match wins and is returned unchanged. A canonical UUID (36 hyphenated or 32 hex) is never
   a prefix: absent means `unknown_task`.
4. Shorter than 8 characters and not exact: `unknown_task`, `reason: prefix_too_short`,
   `min_prefix_length: 8`.
5. Otherwise a completed catalog discovery decides: none → `unknown_task` `reason: no_prefix_match`;
   several → `ambiguous_task_id` with at most 5 `{task_id, title, status, created_at}` candidates and
   `truncated`; a discovery that cannot complete → `reason: prefix_index_unavailable`, never a guess.
   A matched name whose record file has vanished does not count.

`status` additionally reports `task_pending_launch` (launch receipt exists, no record yet) and
`workflow_read_refused` for a managed guard failure. **[V]** `status.refusals`,
`status-batch.rows_and_errors`, `task-list-page.filters`. Pending launches and the managed branch are
**[R]**.

For the direct-only reads the same resolution runs after the authority gate, and a task that has a
record is served to a direct caller even if it is a headless workflow attempt.

## 11. Cursor binding

Cursors are opaque and engine generation-specific; any engine may expire them. The binding rules a
conforming engine must keep (scenario `cursors.binding` and the per-command cases):

| Cursor | Bound to | Mismatch |
| --- | --- | --- |
| `task-list-page` | catalog directory identity (hash of the resolved tasks path), `active_only`, `session_id`, filter scope (predecessor id); not `limit` | `invalid_params` "Invalid listing cursor" **[V]** (`limit` change is accepted) |
| `task-events` | direction, task, log file identity, length and two 64-byte anchors, filter, generation, seq | `event_cursor_invalid` for malformed/other task/changed filter/unknown version; `event_cursor_expired` for a replaced, truncated or vanished log or another root **[V]**; appends never expire it **[V]** |
| `statistics-page` | schema, lexical root, bounds, stable order, generation | `invalid_cursor` for another root or bounds **[V]**; `snapshot_expired` after a relevant mutation **[V]** |
| `interaction-events` | traversal position only | not bound to the root: a cursor from another root is accepted **[V]** (observed in a probe, not pinned by a scenario) **[U6]** |
| `task-artifacts` | task and manifest digest, offset | `artifact_error` "Invalid or stale artifact cursor" **[V]** |

Authority and cursor validity are independent: a refused caller is refused before any cursor is
decoded (events: usage, authority, task, cursor).

## 12. What E1 does and does not establish

Verified against Python 0.0.11 by `conformance/e1/scenarios.py` (31 cases): the read behaviours of
the ten commands, usage and range errors, prefix resolution, cursor round trips and binding, the
direct-only refusal matrix (ancestry caller, environment markers, unverifiable history), the
fail-closed behaviour on incompatible metadata, and that no authoritative document is modified by a
read.

Not established: workflow-managed callers (section 6, items 2-3), session and environment-id
detection methods, `undecidable` identity branches, bounded-index behaviour at scale, concurrent
writers, live-owner log states (`open`, `settling`, `unknown`), capture-loss recovery activity,
retention interaction, and read latency budgets.

## Unknowns and candidate deltas

These are open; the Rust engine implements the baseline above unless the owner records a delta.

- **U1** `interaction-events` collapses all refusals into `unauthorized`. Other direct-only reads use
  `agent_caller` and `caller_undecidable`. A delta would unify the codes (additive for consumers that
  already treat both as refusal).
- **U2** Standalone agent callers can read every task through `status`, `list` and the page read.
  Whether agents should see other tasks' status is an authority decision; Python has not scoped it.
- **U3** Only builders and `delegation` runs scope their members. Workers of other run contracts
  read unscoped. Intended or an artefact of how the guard was introduced is not recorded.
- **U4** One incompatible metadata document disables list/status/recovery/artifact reads for the
  whole root. The contract text elsewhere promises that compatible tasks stay listable with
  `history_incomplete`. The two statements conflict for ctl; the engine follows the code.
- **U5** `artifact_error` is the only code for authority, usage and unavailability refusals.
- **U6** `interaction-events` cursors are not root-bound, unlike the other cursors; consumers are
  told to deduplicate per root.
- **U7** The detection error text can include a damaged task's id; whether that leaks information to
  an agent caller is unassessed (the refusal for direct-only reads is `caller_undecidable` with that
  text).
- **U8** Order of `list` rows is not documented and not pinned; the scenarios compare as sets. The
  page read orders newest `started_at` first, observed, and the scenarios pin it as **[baseline]**.
- **U9** Workflow-managed scoping needs workflow run and receipt seeds (E6) before a conformance
  claim is possible.

Contract surface: none of the above requires a change to `contracts/v1` to implement the baseline.
A resolution of U1, U4, U5 or U6 as a delta would add or change documented codes and therefore needs
a C1 contract decision, fixtures and a coordinated consumer update.

## 13. Who may use the direct-only reads before a positive human credential exists

Question from the Engine session: `statistics-page`, `task-events`, `task-prompt`, `task-changes`,
`interaction-events` and `limit-hits` share `caller_refusal`, which the engine also uses for human-only
gates. Rust has no positive human credential until E5. Which callers may use them?

What the Python gate actually is: a **deny-side, negative-evidence** check. It does not identify a
human. It refuses when an agent is detected (section 2 markers, section 4 ancestry or session) or when
absence cannot be established (4.4), and otherwise admits the caller. Nothing in it proves the caller is
a person; an agent that scrubs its environment and escapes its task's process tree and session passes.
This matches the Engine rule that lineage is deny-side evidence only. The same function also guards
mutating human-only operations (`takeover`, `resources-release` and similar); those are D8 operations,
which Rust must refuse until E5 because a mutation must not rest on negative evidence.

Distinction to keep: **read gates** (the six above plus successors, `task-list-watch`) protect history
and statistics from agents that would otherwise read other tasks; **human-only mutations** (D8) need a
positive credential.

Recommended interim rule for E1, pending owner acceptance:

1. Rust implements sections 2 to 4 unchanged for the read gates. A caller is admitted only when no agent
   is detected and absence is positively established. All refusals keep the Python codes.
2. The admitted class is documented as "no agent detected", never as "verified human". No capability,
   field or message may claim a human credential. Rust does not use this admission for any mutation.
3. D8 operations (including `send --monitor`, `resources-release`, `launch-settle`, `worktree-remove`,
   `scratch clean`) keep refusing in Rust until E5, independent of this rule.
4. When E5 delivers a positive credential, the read gates may stay on negative evidence (parity) or
   require the credential; that is a later C1 delta, not part of E1.

Alternative: refuse all six reads in Rust until E5. It is the strictest reading but removes the history
and statistics views the Mac needs at the E1 exit, with no gain against a determined agent beyond what
the deny-side check already gives. Not recommended; the choice is the owner's (recorded as a question).

`limit-hits` and `task-changes` are not covered by E1 scenarios; their gate is the same function **[R]**.

## 14. Test seam and missing-root inspection (Engine inputs)

- All E1 seeding goes through `conformance/e1/seed.py::seed_reads` (and `init_root`, `write_task`,
  `write_events`) via `Ctx.seeder`, a single replaceable seam, so the scenarios can later seed roots
  with the Engine's fixture tool without changing any case. Cases that add individual documents use the
  same module functions.
- `state-inspect` on a missing root must report `status: "missing"`, `reason: "missing_root"` and create
  nothing. Python reaches this decision without creating any path; a Rust implementation should decide
  by `lstat`-style probing of the root and its ancestors (never following symlinks, never `mkdir`). The
  S07 binary suite already pins the missing-root result.
