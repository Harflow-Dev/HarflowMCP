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
- **Direct caller.** No agent evidence was found after a complete check (section 4.4). This is the absence
  of evidence, not a credential: see section 13.
- **Agent caller.** A confirmed task caller, or a worker environment marker (section 2). Includes
  standalone tasks and workflow-associated tasks.
- **Managed caller.** An agent caller that a workflow association scopes: role `builder`, or any role
  in a run whose `execution_contract` is `delegation` (section 6). Managed callers get a restricted
  view of task reads; every other agent caller does not.
- **Undecidable.** A caller could not be established as present or absent. Always fails closed.
- **Authority-gated read.** A read that consults the caller before answering (section 5 lists them).

All state is under the selected root; every check below runs after the root has been qualified
(Python's ordinary-read behaviour: a missing or empty root is initialized by any ordinary read, a
foreign or incompatible one refuses with `state_incompatible` and is left untouched).
**[V]** `reads.missing_root`, `list.missing_root_initializes`, `list.foreign_root_refused`.
The initialization half conflicts with the Engine's R01 rule that missing-root probes create nothing and
that explicit store initialization is an internal fixture operation: **[U10]**. The scenarios tag it
`[pending U10]`; the foreign-root refusal is not in question.

## 2. Environment signals

`harflow_mcp.takeover.env_agent_refusal` (the cheap check, no state access): the caller is an agent
when **any** of the keys `HARFLOW_TASK_ID`, `PB_TASK_ID`, `HARFLOW_DEPTH`, `PB_DEPTH` is present in the
environment. For the **direct-only gate** presence is the whole test: empty values, malformed depths and
ids of unknown tasks count. The refusal is `agent_caller` (the message wording is not contract).
**[V]** `scope.environment_markers` (ten marker combinations, empty values included, on all six
direct-only reads).

Truthiness is what the other gate families use, so the same empty value gives different answers:
`HARFLOW_TASK_ID=""` refuses `task-events` but leaves `status` and `list` readable, because the managed
reader and the artifact hint test `env.get(...)` for a truthy value. **[V]** `scope.environment_markers`
(empty-marker reads). Rust must keep both tests or record a delta.

Rust treats `PB_TASK_ID` and `PB_DEPTH` as **deny-only** signals: their presence refuses the direct-only
gate, exactly as in Python, but Rust never reads them as an identity, lineage or fallback for any lookup
(the Engine's R01 forbids Python legacy-marker fallbacks in fresh roots). In Python `PB_TASK_ID` is
likewise only a presence signal for reads. **[V]**

Only a **non-empty** `HARFLOW_TASK_ID` (not `PB_TASK_ID`, not the depth variables) influences the managed
paths (section 4): when it is set and detection finds no confirmed caller, `status`, `status-batch`, `list` and
`recovery-activity` fail with `workflow_read_refused`, the page read with `invalid_params` (it raises a
plain error that the page handler maps), and `task-artifacts` with `artifact_error`. A set `PB_TASK_ID` or
depth variable alone does not stop `status` or `list`.
**[V]** `scope.unverifiable_task_identity`.

Mutation callers use the stricter `caller_policy.signals`: values are validated (task id charset,
legacy id must equal the current id, depths ASCII decimal) and a present signal requires a live,
related, confirmed parent whose id equals `HARFLOW_TASK_ID`. This is outside the read scope and is
recorded here only so the two uses of the markers are not confused. **[R]**

## 3. Process identity

Used to confirm that a task record still denotes a live process (`identity.py`).

- Lookup: `ps -o lstart=,command= -p PID` with `LC_ALL=C`, bounded to 5 s. **This record format binds
  Python roots only.** Rust uses its own process identity (PID plus high-resolution start identity and boot
  generation, R01) and need not read or write `lstart` text; it must produce the same alive/dead/undecidable
  verdict for the same facts. The legacy branches below (no recorded start time) exist only for Python
  records; fresh Rust roots have none, and an adapter may report them unsupported.
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

**[V]** `scope.identity_verdicts` registers the test process under each identity state through the fixture
port: `alive` (with and without markers), a reused pid (`dead`), missing markers (`undecidable`), the legacy
no-start-time forms (`dead` or `undecidable`), and a terminal record with and without an exit code (excluded
or kept as a candidate). Outcomes: alive → `agent_caller`; dead or observed-terminal → treated as direct;
undecidable → `caller_undecidable` (direct-only reads), `workflow_read_refused` (`status`, `list`),
`invalid_params` (page read), `unauthorized` (`interaction-events`), `artifact_error` (`task-artifacts`).
`caller_process_titles` and `ps` failures are **[R]**.

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
**[V]** ancestry (`scope.agent_caller_by_ancestry`, `scope.identity_verdicts`). The session method and the
environment-id method are **[R]**: the harness cannot place the engine in a task's session, and the only
related record it can create is its own ancestor. `scope.unverifiable_task_identity` shows only that a
confirmed ancestry caller is not displaced by an unrelated environment id; the order among the three
methods is not proven.

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
confirmed `dead` is undecidable. An **undecidable caller makes the page read fail** with `invalid_params`
(the raised error is mapped by the page handler), **[V]** `scope.identity_verdicts`,
`scope.unverifiable_task_identity`. An **incomplete catalog** is a different path: while indexing is
unfinished the shared preparation response of section 8 is returned, and when the catalog is blocked the
page is a successful empty response with `authority_incomplete: true`, **[V]**
`scope.incompatible_metadata_fails_closed`. Only the incomplete-catalog paths withhold items; they are
not errors.

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
| `task-list-page` (without the predecessor option) | managed page reader (section 6) | `invalid_params` when the caller is undecidable; an incomplete catalog returns an empty withheld page instead, not an error | open to non-managed agent callers **[V]** |
| `task-artifacts` | artifact caller (section 9) | `artifact_error` for every refusal | a task caller reads only its own |
| live `workflow-inspect --view activity` | direct caller only (`managed is None` and no `caller_refusal`) | no refusal: other callers get the settled-only view | not covered by E1 **[R]** |
| `workflow-artifacts` (run level) | human (no `caller_refusal`) or the run's orchestrator | plain error "available to a human caller or the run's orchestrator" | not covered by E1 **[R]** |
| `workflow-abandon-dispatch` | direct-only | plain error "Only a verified human may abandon an uncertain dispatch" | not covered by E1 **[R]** |
| `send --monitor`, `cancel-message --monitor`, approval and conversation `--monitor` | direct-only | `caller_refused` (send) | mutations; D8, refuse in Rust until E5 **[R]** |

`status`, `list` and the page read are therefore open to a standalone agent caller over **all** tasks.
Only history-class reads (events, prompt, changes, statistics, interaction feed, successors) are refused
to agents. **[V]** `scope.agent_caller_by_ancestry` (tagged `[baseline, pending U2]`).

What `status` exposes to that caller for any task, beyond identity and state: `last_output_tail`,
`summary`, `raw_stream_log` and `events_log` (paths of the raw and normalized logs), `resume_command`
(including the repository path and session id), `messages` and queued follow-up counts, and
`recovery_activity`. The scenarios assert these keys are present for a standalone agent; whether that is
acceptable is **[U2]**. History-class reads exist to withhold exactly this kind of content, so the two
families are inconsistent.

The exit status of a refusal is 1; usage errors are 2 and precede authority. **[V]**
Error messages are not contract: the scenarios assert `error.code` and that a refused caller learns
nothing about ids, never the English text.

## 6. Managed callers

`workflow_inspection.managed_reader` (used by `status`, `status-batch`, `list`, `recovery-activity`):

1. Detect the caller with `detect_caller`; if none, run `detect_caller_detail`. An undecidable result
   raises "Workflow caller authority is undecidable: REASON"; no caller and a set `HARFLOW_TASK_ID`
   raises "Workflow caller task identity cannot be verified"; no caller otherwise returns no
   management (direct view). Both raises surface as `workflow_read_refused`. **[V]**
2. With a caller: look up its workflow association strictly (receipt file
   `workflow-owners/TASK_ID.json`, then inherited ownership through `parent_task_id`/`spawned_by`
   ancestors). `managed_reader` calls `WorkflowStore.task_owner(task_id, strict=True)`; its exact ancestry
   bound is not recorded here **[U9]**. The 64-visit limit, cycle error and overrun error belong only to
   the bounded `managed_page_reader` (`workflow_inspection.py:262-282`). No association returns no
   management (direct view): a plain standalone agent is **not** scoped. **[V]** (standalone case)
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
run documents and receipts. **[R]**

### 6.1 Interim rule for Rust before workflow ownership is readable (proposed, owner decision)

Python treats "no association found" as a direct view. That is safe only because Python can always read
associations. A Rust engine that cannot yet read workflow ownership cannot tell "no association" from
"association unreadable", so copying Python's fallthrough would fail open and show a workflow worker the
whole root. **Proposed [proposed]:** until E6 supplies association reading, an agent caller (any confirmed or
marker-detected agent) whose workflow ownership is unresolved is refused on every authority-gated read
(`status`, `status-batch`, `list`, `recovery-activity` with `workflow_read_refused`; the page read with
`invalid_params`; `task-artifacts` with `artifact_error`). A caller with no agent evidence is unaffected.
This is stricter than Python for standalone agents and is a deliberate interim difference, to be replaced
by the Python rule when E6 lands. It needs owner acceptance, together with U2.

E6 managed-caller scenarios (not in E1): builder scope; orchestrator in a `delegation` run; worker node
reading only its own execution; workers of non-delegation runs (U3); inherited ownership through ancestors
including cycle and overrun; closed-namespace id resolution and ambiguity inside it; page filtering,
cursor removal and recomputed counts; receipt incompatible and unavailable codes; managed artifact reads;
live `workflow-inspect` and run-level workflow artifact gates.

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
| `task-list-page` | success with the withheld page: `items: []`, `related_headers: []`, `next_cursor: null`, `has_more: false`, `counts_complete: false`, `authority_incomplete: true`, `history_incomplete: true`, `note`, and `catalog_state` limited to `status` (`blocked`), `source`, `reason`. This comes from the blocked catalog, not from the caller check (an undecidable caller is `invalid_params`, section 4.5) | none |

**[V]** `scope.incompatible_metadata_fails_closed` (tagged `[baseline, U4]`). The detection error text can name
the damaged task id; the scenarios do not pin the message. The contract also says `status` reports
`task_incompatible` for a damaged document and that lists may display compatible unrelated tasks while
reporting `history_incomplete`. Neither is reachable through ctl in Python: the caller check refuses first
(`status` of the damaged id is `workflow_read_refused`; `task_incompatible` never appears). **[U4]**

## 8. Cold index and preparation

The first bounded read on a root builds a derived listing index. While it is not ready, the page read
returns the shared preparation response (contract "Bounded read authority preparation"): `items: []`,
`related_headers: []`, `next_cursor: null`, `has_more: false`, `counts_complete: false`,
`bootstrap_pending: true`, `catalog_state` holding only `status`, `source`, `reason`; no counts, no
blocker types and no other catalog field, even for a caller later shown to be direct. Clients re-issue
the read. **[V]** (the scenarios re-issue the read under a harness policy of 40 attempts at 250 ms and
check this shape whenever it appears; Python showed it on the first call of a cold root, so a conforming
engine may or may not produce it). The retry budget is harness policy, not contract. `statistics-page` has
the analogous `state: "preparing"`, which Python never showed in these runs **[R]**.

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
  **[V]** `task-artifacts.availability_and_prefixes`, `scope.agent_caller_by_ancestry`.
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
| `task-events` | direction, task, log file identity, length and two 64-byte anchors, filter, generation, seq | `event_cursor_invalid` for malformed/other task/changed filter/unknown version; `event_cursor_expired` for a replaced, truncated or vanished log or another root. **[V]** for malformed, oversized, other task, changed filter, truncated, vanished and another root, and for appends not expiring it; the unknown-version and anchor-content cases are **[R]** |
| `statistics-page` | schema, lexical root, bounds, stable order, generation | `invalid_cursor` for another root or bounds **[V]**; `snapshot_expired` after a relevant mutation **[V]** |
| `interaction-events` | traversal position only | not bound to the root: a cursor from another root is accepted **[V]** (`cursors.binding`, `[baseline, U6]`) |
| `task-artifacts` | task and manifest digest, offset | `artifact_error` for a malformed cursor and for a cursor whose manifest changed **[V]** |

Authority and cursor validity are independent: a refused caller is refused before any cursor is
decoded (events: usage, authority, task, cursor).

## 12. What E1 does and does not establish

Verified against Python 0.0.11 by `conformance/e1/scenarios.py` (33 cases): the read behaviours of
the ten commands, usage and range errors, prefix resolution, cursor round trips and binding, the
direct-only refusal matrix (ancestry caller, environment markers, unverifiable history), the
fail-closed behaviour on incompatible metadata, and that no authoritative document is modified by a
read.

Not established: workflow-managed callers (section 6, items 2-3; see 6.1 for the E6 scenario list), the
session and environment-id detection methods, `ps` failure and `caller_process_titles` branches,
bounded-index behaviour at scale, concurrent writers, live-owner log states (`open`, `settling`,
`unknown`), capture-loss recovery activity (not seedable through the fixture port), retention
interaction, `limit-hits` and `task-changes`, and read latency budgets.

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
  whole root. The contract text elsewhere says `status` reports `task_incompatible` and that compatible
  tasks stay listable with `history_incomplete`. This is the one place where the code and the contract
  text disagree, so the header rule ("contracts win") cannot be applied silently: Rust follows the
  fail-closed behaviour as an interim baseline, tagged `[baseline, U4]`, until the owner decides whether
  the contract sentence or the code is the intent. A decision for the sentence is a C1 delta.
- **U5** `artifact_error` is the only code for authority, usage and unavailability refusals.
- **U6** `interaction-events` cursors are not root-bound, unlike the other cursors; consumers are
  told to deduplicate per root.
- **U7** The detection error text can include a damaged task's id; whether that leaks information to
  an agent caller is unassessed (the refusal for direct-only reads is `caller_undecidable` with that
  text).
- **U8** Order of `list` rows is not documented and not pinned; the scenarios compare as sets. The
  page read orders newest `started_at` first, observed, and the scenarios pin it as **[baseline]**.
- **U9** Workflow-managed scoping needs workflow run and receipt seeds (E6) before a conformance
  claim is possible; the ancestry bound of `managed_reader`'s strict owner lookup is unrecorded.
- **U10** Python initializes a missing root on any ordinary read. The Engine's R01 says missing-root probes
  create nothing and store initialization is an internal fixture operation. The scenarios
  `reads.missing_root` and `list.missing_root_initializes` are tagged `[pending U10]`; the owner must say
  whether ordinary reads of a missing Rust root answer empty without creating it, or refuse. Either is a
  delta against the contract text if it changes the observable root.

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
   is detected and no agent evidence remains after a complete check (section 4.4). All refusals keep
   the Python codes.
2. The admitted class is documented as "no agent detected", never as "verified human". No capability,
   field or message may claim a human credential. Rust does not use this admission for any mutation.
3. D8 operations (including `send --monitor`, `resources-release`, `launch-settle`, `worktree-remove`,
   `scratch clean`, `workflow-abandon-dispatch`, `mcp-allowlist` edits) keep refusing in Rust until E5,
   independent of this rule. The gate list of section 5 also names live `workflow-inspect`, run-level
   workflow artifacts and `workflow-notifications`: these read-class gates follow rule 1 but are outside
   E1 and need their own scenarios.
4. When E5 delivers a positive credential, the read gates may stay on negative evidence (parity) or
   require the credential; that is a later C1 delta, not part of E1.

Alternative: refuse all six reads in Rust until E5. It is the strictest reading but removes the history
and statistics views the Mac needs at the E1 exit, with no gain against a determined agent beyond what
the deny-side check already gives. Not recommended; the choice is the owner's (recorded as a question).

`limit-hits` and `task-changes` are not covered by E1 scenarios; their gate is the same function **[R]**.

## 14. Fixture port and missing-root inspection

- The E1 cases create, damage and inspect state only through the **fixture port** (`conformance/e1/port.py`):
  `seed_world`, `add_task`, `add_live_caller(pid)`, `damage_task`, `append_event`, `append_malformed_line`,
  `append_partial_line`, `truncate_log`, `remove_log`, `tamper_prompt`, `write_artifacts`,
  `change_artifact_target`, `authoritative_digest`, plus root helpers (`initialize_empty`, `remove_root`,
  `make_foreign_root`, `root_state`). No case writes a file under the state root, and no file layout is part
  of the interface (R01 forbids a Python file-layout API for Rust). A self-check fails if a case names a
  layout path.
- The **Python adapter** (`PythonLayoutAdapter`) is the only code that knows Python 0.0.11's files.
- The **Rust adapter** will come from the Engine session as an executable invoked `ADAPTER OP --root ROOT`
  with JSON arguments on stdin and a JSON result on stdout. The runner takes `--fixture-adapter PATH
  --fixture-adapter-sha256 DIGEST`, refuses an unpinned or mismatching tool, and the tool must not be
  reachable from the public engine binary.
- `authoritative_digest` must be a non-empty digest of all authoritative state (derived caches excluded) and
  must differ between a seeded and an empty store; the harness fails a case whose digest is empty or constant,
  so another layout cannot make the "reads do not modify state" invariant vacuous.
- `add_live_caller` takes semantic identity states, not `lstart` text; an adapter that cannot express one
  returns unsupported and the case records a skip.
- `state-inspect` on a missing root must report `status: "missing"`, `reason: "missing_root"` and create
  nothing. Python decides this without creating any path; a Rust implementation should decide by
  `lstat`-style probing of the root and its ancestors (never following symlinks, never `mkdir`). The S07
  binary suite already pins the missing-root result.
