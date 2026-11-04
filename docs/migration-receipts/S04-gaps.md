# S04 follow-up — Electron-facing ctl read gaps

Date: 2026-11-04 (Asia/Jakarta). Platform: macOS arm64. Lead: HarflowMCP. Co-owned: none.

## Tracking reconciliation — 2026-11-04

This receipt's bounded work is delivered. Its historical results and owner
acceptance status are unchanged; this documentation update adds no test evidence
or scope approval. See the optional [workspace reconciliation](../../../HARFLOW_MIGRATION_CHECKLIST.md#receipt-reconciliation--2026-11-04).

## Disposition and boundaries

This receipt records the follow-up to [S04](S04.md) that closes the read gaps the S04
receipt left open for the Electron client: successor discovery and running workflow
node activity (G-HISTORY), statistics cost and observed model (G-STATS), and stored
bounded diffs (G-DIFF, decisions S04-D3 to S04-D5). It is submitted for owner
acceptance.

All changes are additive ctl reads and optional fields, each behind its own
capability. No MCP tool output changes; the settled MCP node-activity page is pinned
by a golden digest test. There were no paid calls, network probes, live-state or
registration changes, pushes, releases, branches or worktrees. Tests used isolated
temporary state roots and repositories.

## Identities

- Start: HarflowMCP `14c028e` (S04 receipt), clean.
- Plan: revision 3, reviewed in two rounds (kept outside the repository).
- Final code commit: `73182381fb597a7ff4664fc230dd006c20c31d8f`. Product version
  `0.0.7`; the release version bump is not part of this work.

| Commit | Change |
| --- | --- |
| `fe82b56` | `task-list-page --predecessor-task-id` (`task-successors.v1`) |
| `b94dd12` | Live activity for running workflow nodes (`workflow-live-activity.v1`) |
| `d9dae72` | Statistics cost and observed model (`statistics-cost-model.v1`) |
| `75457b3` | Live activity log read through one descriptor; cost overflow; stricter gate |
| `fe598c7` | `task-changes` stored bounded diffs (`task-changes.v1`) |
| `6daf893` | Capture and reader hardening (review round 1) |
| `430d05c` | Cut-off listings, typechange alignment (review round 2) |
| `3689983` | Working-tree index built from the pinned head (owner-approved redesign) |
| `822f215` | Work-tree gate restored; bounded index parsing; advisory conflicts |
| `7318238` | Byte-bounded conflict reports; bounded object-store walk |

## Delivered contract

Specified in `docs/contracts.md`; capabilities appended in this order to
`wire_contracts.CAPABILITIES` and `contracts/v1/version-info.json`:

- **`task-successors.v1`**: `task-list-page --predecessor-task-id ID` lists tasks that
  resumed or continued a task. Direct callers only when the flag is used. Successors
  still waiting in a pending launch have no record and are not listed
  (`successor_scope: "recorded_tasks"`). The predicate is unindexed; accepted at
  current scale.
- **`workflow-live-activity.v1`**: `workflow-inspect --view activity` reads running,
  starting, reserved and uncertain attempts for a verified direct caller with no
  managed view, adding `attempt_status`, `settled` and `activity_state`. MCP and
  orchestrator reads keep the settled-only guard. `task-events` serves any task with
  a record, including headless workflow attempts.
- **`statistics-cost-model.v1`**: execution facts gain `cost_usd`, `cost_provenance`,
  `cost_additive`, `observed_model` and `observed_model_provenance`; pages gain
  `cost_coverage`. Null means unknown, never zero. `requested_model` is never
  replaced. Native rows carry no cost; their transport's cost includes them.
- **`task-changes.v1`**: after settlement the task supervisor captures, within a 2 s
  budget, a committed-range and a working-tree diff per observed repository into
  `{task_id}.changes.json`; `harflow-ctl task-changes` pages rows and file text for
  direct callers. States, reasons, limits, cursor rules and security rules are in
  the contract.

## Owner decisions in this follow-up

| ID | Decision |
| --- | --- |
| G-D1 | Implement all four gaps (2026-11-04). |
| G-D2 | Stage 4 core implemented in parallel with stages 1–3. |
| G-D3 | Working-tree capture never gives the agent's index to Git: a private index is built with `read-tree <pinned head>` and refreshed against the work tree. Third review round approved. |

Consequences of G-D3, documented in the contract: every tracked file is hashed
within the 2 s budget, so very large checkouts (roughly 65k+ files, measured) end
`incomplete` / `capture_timeout`; staged-but-uncommitted files appear as untracked
names; conflicts read from the agent's index are advisory only
(`reported_conflicts`, bounded to 100 names and 8 KiB escaped per scope).

## Contract and fixture identities

- Manifest sha256 after each stage: `04d71a97…` (stage 1), `4d34212a…` (stage 2),
  `e22b9cf9…` (stage 3); final `06094a8f210130d0794afe3913d66c853baf8d43c9022fd3140e22ebfe0aacaa`.
- New fixtures: `ctl-task-page-successors-*` (6), `ctl-workflow-inspect-live-*` (5),
  `ctl-statistics-page-cost-reported-and-silent.json`, `ctl-task-changes-*` (20).
- Changed fixtures: 13 `ctl-task-events-*` in `change_token` only (task records grew
  two optional fields), plus `version-info.json`, `manifest.json` and
  `manifest.sha256`. `CHANGELOG.md` lists the re-vendor set.
- Static `ctl-statistics-page-{empty,preparing,ready,unavailable}` examples are
  unchanged and predate the new optional fields.

## Commands and results

- Targeted suites per stage and per fix pass (last: 898 passed, 1 skipped on macOS
  for non-UTF-8 filenames). Both fixture generators pass `--check`; legacy-name and
  private-ref guards pass.
- Full suite on an extracted copy of `7318238` with `PYTHONPATH` at its `src`:
  13489 passed, 559 skipped, 6 failed. All 6 failures are in
  `tests/test_runtime_loaders.py`, which these commits do not touch; they assert
  path patterns that do not hold under the `/private/tmp` extraction. In the
  checkout the same file passes 14 of 14.

## Reviews

Reviewers: Opus subagent, Codex `gpt-6-astra` (low, read-only), Vibe (read-only),
and the automated commit security review.

- **Plan**: two rounds. Round 1 blocker (live gate used `managed is None`) and
  majors on git hardening, index, retention lock, page bounds, cost sources and
  state derivation were all accepted. Round 2 findings (owner-death precedence,
  distinct timeout and size causes, cost coverage tied to execution rather than
  token qualification, observed-model sources) were folded in.
- **Stages 1–3**: Opus and Vibe approved in round 1; Codex found a check-then-open
  race on the live log and a cost overflow. Fixed in `75457b3`; all three approved
  in round 2.
- **Stage 4**: rounds 1–3 plus a verification pass. Accepted and fixed, among
  others: an index entry `../secret.txt` reading outside the worktree (Opus,
  reproduced); agent index flags, forged stat data and sparse entries hiding edits
  and parser differentials (security review), resolved by the G-D3 redesign;
  malformed documents crashing the reader and listing pages over 262144 bytes
  (Codex); typechange alignment and lost exclude rules (Opus); missing index and
  cursor `RecursionError` (Vibe); a dropped work-tree gate that would have let
  `core.worktree` aim the diff at another directory, conflict names hiding rows,
  object-store symlinks and alternates, unbounded varint and enumeration
  (security review, Codex, Opus). Final verification of `7318238`: Opus and Codex
  approve. Vibe's round-2 run failed with a model `ReadError`; its round-3 findings
  are covered by `822f215`.
- Nothing was rejected. Opus nits left as accepted: a store over the 100,000-entry
  walk cap reports `object_store_untrusted` rather than a size reason, and a stray
  file directly under `objects/` refuses the store.

## Remaining gaps and caveats

- Large checkouts can end `incomplete` / `capture_timeout` (G-D3).
- Hard links in an object store are not detected.
- A linked worktree that Harflow did not provision cannot be anchored to dispatch.
- Repositories using alternates (for example `clone --reference`) get no capture.
- `GitProbe` baseline status still ignores `info/exclude` (pre-existing).
- Statistics: all-time history beyond retention is still unavailable.
- S17 design states for loss, reconnect and unavailable remain open.

## Handoff

- **Release**: the engine release that ships these capabilities needs its version
  bump and changelog section; clients gate on the capabilities above.
- **S06**: HarflowMac re-vendors the complete bundle, byte-identical with manifest
  digest `06094a8f…`, when it moves to this engine.
- **Electron**: consume these reads through the transport-neutral ports
  (HarflowDesktop#1) and handle `incomplete`, `not_captured` and
  `unmerged_unknown` in the changes view.
