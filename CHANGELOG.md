# Changelog

## Unreleased

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
