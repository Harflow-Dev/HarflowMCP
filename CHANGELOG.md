# Changelog

## Unreleased

- Add a persisted resource reservation ledger (`resources.py`): named exclusive locks with strict per-name FIFO, all-or-nothing grants, borrowing from an enclosing scope, a settle-only reaper and loud refusal of a corrupt ledger, plus the additive ctl commands `resources`, `resources-release` and `resources-repair` (the last two for a verified person only). Nothing dispatches against it yet; the task and workflow integration follows (#10, #3).

## 0.0.3

- Retry verified task metadata replacements during attached cancellation and recovered cancellation delivery, settlement and handoff, retaining bounded retries and refusal before effects on malformed or persistently changing metadata.

- Support Vibe 3.0.1 dispatch with `ask` instead of the removed `plan` profile, disclose its weaker approval-only policy, and qualify native start/resume, large stdin, denials and failures against a local fake provider.

- Address review follow-ups: separate dispatcher liveness from observer mode, move bounded plan metadata work off stdout draining, retain plan directory identity for environment-independent availability, report observed settled sends precisely, and make successor history lookup opt-in during status polling.

- Distinguish dispatch origin, detached execution owner and responding server so normal fresh attachments are not labelled recovered; retain explicit unknown identity and independent liveness evidence (#54).
- Preserve partial Git observations, report bounded phase/probe diagnostics and fix large packed-refs status inspection without increasing probe budgets (#55).

- Explain sealed live input without calling a still-running task finished, retain closure reasons, and expose paged reverse resume/continuation links (#47).
- Add neutral publication intent with a deprecated identical alias, report the checkout sampled at dispatch, and accept validated unrestricted extra paths as a disclosed no-op across launch and recovery (#49).
- Disclose Claude plan-mode file writes and record successful observed built-in plans as metadata-only artifacts; qualify the behavior with no-paid Claude 2.1.310 probes (#50).
- Release matching dead-owner/dead-agent session reservations for context-preserving resume, including attached tasks; preflight 256 MiB free per required filesystem and preserve draining/recovery evidence after disk-write failures (#52).

## 0.0.2

- Retry transient task metadata replacements during workflow ownership, attachment, continuation and live-message observations while retaining bounded reads, ownership checks and refusal on persistent uncertainty.

- Clarify harness and Harflow interaction-channel terminology in MCP descriptions and CLI help while preserving public field and option names.

- Allow the Mac Monitor response channel to answer caller-owned approval requests on engines advertising `caller-approvals.monitor-respond.v1`. Preserve `decided_by` semantics and add optional channel-derived `responder`; expose the existing bounded, untrusted decision reason fields and `awaiting_decision_count` without changing `pending_count` or `interaction_state`. Approval remains separate from delivery and execution: delivery-time path and protected-target checks can still convert a recorded approval to an effective Harflow rejection. MCP remains a caller channel and cannot claim Monitor authority. Authority remains inferred and cooperative, not authenticated.

- Re-check approval paths at delivery and protect `.agents` for Codex file changes (#44): the pump re-resolves every path of an approved Read, Write, Edit or Codex file change through the backend and re-runs the protected-target checks immediately before writing the approve, and refuses with a Harflow rejection (`path_changed_since_admission`, or `protected_path` when a target became protected) if the resolved paths differ from the admission snapshot, a path set changed or the snapshot is missing or malformed (a path that cannot be resolved is refused as `protected_path`); the caller's `decision` stays recorded, the row ends `system_rejected` and classifies as `harflow_policy_rejected`. Rows ending in rejections the caller chose and command rows are unaffected, and the private snapshot is not exposed. Codex file changes also refuse `.agents` in every directory from the Git root to the working directory and in linked-worktree roots, alongside `.codex`. A hard link to a protected file and the agent's own write after delivery remain outside the check. No wire contract changes.

- Add caller approvals for Codex through `codex app-server` (macOS and codex-cli 0.163.0 only, `write_in_repo` and `publish`): `caller_approvals` runs that task on the app-server transport with the approval policy, reviewer, sandbox, writable roots, network, hooks and shell environment pinned and verified from the thread response, and a caller approves or rejects each exact command; with `caller_write_approvals` also each exact patch (paths, kinds and diff frozen, protected targets including `CODEX_HOME` and every `.codex` from the Git root to the working directory refused, a differing completion recorded as `mismatch` and stopping the run). Repository `.codex` configuration is ignored by untrusted project overrides for the working directory, its ancestors and the Git root, resolved with repository Git configuration neutralised (a linked worktree also overrides and scans its main checkout, and an unverifiable `.git` file is refused). Dispatch refuses a `CODEX_HOME` inside a sandbox-writable location, a `profile`, and any rules file (in `CODEX_HOME` or a project `.codex/rules`) that is not only `prefix_rule` calls with a literal `prompt` or `forbidden` decision. An approved command may run outside the OS sandbox and can modify Git config and hooks, `CODEX_HOME` and Harflow state; MCP, permission and user-input requests are always rejected; saved workflow candidates still refuse the opt-in. The default `codex exec` path is unchanged. Codex denials are now classified from Harflow's own evidence (`caller_rejected`, `harflow_policy_rejected`, `policy_denied` for an exec-policy `forbidden` rule, and `approval_policy_never` on the exec path); everything else stays `unclassified`. `caller_approval_support` for Codex lists the qualified version and platform, and `preview_task_permissions` reports the `codex_app_server` transport. Opt-in real-CLI tests and a schema drift test cover the transport.

- `preview_task_permissions` now takes `approval_transport` and `approval_limitations` from the backend. Claude's values are unchanged; for every other backend `approval_limitations` is now "Caller approvals are not available for this backend." instead of Claude's text. Internally, approval frame routing, request ids, path extraction, write tools, extra protected targets and an optional bridge-owned control pipe are backend-provided seams (defaults fail closed), and a task snapshot's `caller_approved_writes` entry gains an additive `file_paths` list only for an action touching several paths.

- Document caller-approval timeout and recovery semantics: the request deadline is the earlier of 1800 seconds and the attempt deadline, expiry delivers a Harflow rejection and never approves, delivery and outcome are recorded separately, and recovery after a denial or expiry is always a new `resume_task`/`continue_task` execution guided by `denial_remedy`, never an approval (`allowed_commands` covers only Bash parts; workflow nodes blocked by a permission denial need a new run or an updated definition). Correct the `respond_approval_request` docstring and record the remaining limits (workflow permission-grant retry, approval notifications, Monitor presentation, non-Claude backends, native-child approvals). Close a start-time authority gap: an agent caller starting a saved workflow is now also checked against the workflow's on-demand candidates for `allowed_commands` and `caller_write_approvals` beyond its own.

- Accept `allowed_commands` on saved workflow agent candidates (primary, fallbacks and on-demand nodes): validated at save and launch for each candidate's backend and freedom, qualified at dispatch and passed to the node's task, with a fallback using its own grants. Agent callers cannot save or start grants beyond their own, the candidate signature and session compatibility include the field only when non-empty, native execution runs such nodes Headless, and orchestrators, builders and `start_workflow` still refuse the field.

- Re-resolve the protected Write and Edit approval targets at every admission, so a mid-run `core.hooksPath` change is honored, and protect the whole repository `.claude/` directory (hooks, agents, commands and settings) from caller-approved writes.

- Add `allowed_commands` to `start_task`, `resume_task`, `continue_task` and `preview_task_permissions` (`harflow-ctl run`, `resume`, `continue` and `permission-preview --allowed-command`, repeatable): a bounded list of Bash command patterns granted for one run on Claude Code 2.1.310 at `write_in_repo` and `publish`, so a task can run a test runner or build without `unrestricted`. A conservative grammar refuses shells, wrappers, interpreter evaluation, `git` everywhere and `gh` below `publish`, and keeps paths inside the repository; it is a guard, not a security boundary. Grants join one comma-separated `--allowedTools` value next to the unchanged `--disallowedTools`, so the access level's `git commit`/`git push` denies still win (measured). Omitted grants inherit on resume and continuation, an explicit list replaces them, and a nested dispatch may grant only exact patterns its caller holds (unless unrestricted). The run is re-qualified at every launch and stopped with a bridge notice if its init event disagrees on the Claude Code version. `enforcement.extra_allowed_commands`, `allowed_commands` on task snapshots and records, and `capabilities.allowed_commands` are additive; other backends refuse the field.

- Qualify Claude Code 2.1.310 for caller approvals from the single measured table and cross-check the version reported by the run's init event against the version qualified before launch, stopping the run and invalidating pending approvals on a mismatch. Read requests, including path-safety and out-of-repository Reads, are now approvable; **behavior change:** existing `caller_approvals` users on 2.1.310 will be asked about out-of-repository Reads where the run previously stopped. Add the separate `caller_write_approvals` opt-in (`start_task`, `preview_task_permissions`, `harflow-ctl run`/`permission-preview --caller-write-approvals`, workflow candidates) for exact-action Write and Edit approvals, with strict schemas, protected paths, Harflow system rejections (`write_approvals_not_enabled`, `protected_path`, `request_too_large`) delivered through the existing ledger, `enforcement.caller_approval_tools`, and `caller_approved_writes` disclosure. 2.1.302 stays Bash-only.

- Classify Claude permission denials from the exact refusal texts measured on 2.1.310: `permission_denials` entries gain `denial_category`, `denial_remedy` and `classification_basis`, tasks gain `denial_categories`, and the `task_finished` event and workflow result evidence carry them. Only denials confirmed by a result event are classified; stream-only matches, unmeasured versions and other backends stay `unclassified`. All fields are additive and retry gating is unchanged.

- Advance the local engine identity to 0.0.2; add explicit private-format storage compatibility, exclusive writer-line promotion, narrow empty-directory adoption, and inspect/confirmed crash-resumable incompatible-state backup/reset without relaxing live runtime gates.

- Advance clean final workflow branches to their own joins or End, support `always_route` opt-outs and validated child entry assignment pass-through, and persist runner decision/transition provenance atomically with token movement.

- Shape workflow completion offers from the current End, pending-token, join, required-failure, and goal-replanning state; explain blockers in both optimized and legacy orchestrator prompts.
- Add acknowledged workflow section references and exact-prefix append delivery, purpose-based orchestrator graph projection, and bounded current-run instruction inspection while preserving complete worker instructions and v1 public contracts.

- Keep task output, live input and automatic continuation with detached task owners across MCP server restarts; reserve launch and session identities before execution.
- Add explicit interrupted workflow-scheduler recovery with retained decisions, existing task adoption, linked-tree capacity restoration and native observation settlement barriers.

- Select Vibe's Python CLI entrypoint per invocation and validate backend environment contributions at the spawn boundary.

- Add local package validation and gated PyPI/MCP registry release workflows, with a registry-compatible server alias.

- Document the companion app’s exact paired managed runtime and preserve the separate automatic-opening limitation.

- Set the initial product version to 0.0.1 while preserving v1 wire and storage contracts.

- Qualify the shared notification database before task/workflow access; atomically bootstrap fresh storage, refuse incomplete or foreign schemas and WAL state, and preserve narrowly qualified native rollback recovery.

- Qualify task approval ledgers and request task identities with a bounded Harflow header; refuse incompatible sources before decisions while preserving action digests, permanent locks and existing delivery gates.
- Qualify flat task artifact manifests with filename-bound Harflow identity and finite JSON; refuse unqualified or incompatible manifests without overwrite while preserving metadata-only references, cursor revisions and existing budgets.
- Qualify individual live-input inbox rows with filename-bound Harflow envelopes before append; skip unqualified rows while preserving permanent locks, byte offsets, close markers and existing delivery evidence.
- Qualify parsed control phase payloads with filename-bound Harflow envelopes and stable-source reads; preserve existing marker-presence, lease, signal and retention semantics, and refuse older flat development payload authority.
- Qualify next-continuation follow-up ledgers with filename-bound Harflow envelopes and bounded source checks; refuse unqualified or changed sources before queue mutation, continuation launch or retention deletion.
- Qualify workflow ownership receipts with filename-bound identity, bounded source checks and stable refusal diagnostics across control and display consumers.
- Preflight receipt targets before run publication, refuse changed targets without overwrite, and stop same-run supervisor relaunch after blocked settlement while preserving ordered partial-write evidence.
- Qualify workflow run documents before storage, control, catalog ownership and retention decisions; preserve authored snapshots and public run payloads.
- Refuse incompatible or unavailable run sources with stable diagnostics and distinguish published runs from incomplete subsequent bookkeeping.

- Preserve multi-task caller cancellation during concurrent metadata/existence I/O completion and progress delivery on Python 3.11.
- Retry caller candidate acquisition once when every refused task source changed during reading, preserving strict complete-history and direct-read checks.
- Qualify saved workflow definitions and delegation migration backups with Harflow product identity and stable major/minor compatibility. Refuse unqualified sources and observable source changes before mutation.

- Qualify task metadata with `harflow.task` identity and stable major/minor compatibility; refuse older unqualified development task documents without adoption.
- Report task incompatibility and I/O unavailability separately, quarantine stale task-catalog authority, and expose incomplete history in listings and cancellation cascades.
- Prevent stale task updates and failed final persistence from authorizing overwrite or automatic continuation while preserving process cleanup.

- Mark fresh engine stores and enforce compatible shared ownership across processes, operations and background writers.
- Refuse nonempty unmarked or incompatible stores with explicit `state_incompatible` errors; report bounded ownership contention as `state_busy`.

- Centralize engine state-root selection and propagate `HARFLOW_STATE_ROOT` to agents and workflow supervisors.
- Refuse invalid selected roots at startup and unsupported injected task layouts before launch.
- Normalize relative explicit state paths and relative HOME defaults to absolute lexical paths against the working directory.

- Define explicit Harflow v1 ctl, setup and normalized event envelopes with product identity.
- Add side-effect-free version-info compatibility reporting and canonical cross-language fixtures.
- Emit structured setup process and usage errors when JSON output is requested.

- Extract the engine into HarflowMCP with independent package, command, environment, and state identities.
- Preserve current wire-format versions during extraction.
- Require requalification before enabling renamed native workflow adapters or extra writable path grants.

### Task supervision and background recovery

- Disclose independent owner/process verdicts, capture continuity and raw-log cutoff.
- Add bounded exact-session Claude transcript activity with independent display cursors.
- Persist configurable background idle bounds across dispatch and continuation, with precise idle-bound diagnostics.
