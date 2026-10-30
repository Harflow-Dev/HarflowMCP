# Harflow public wire contracts

HarflowMCP 0.0.5 is a pre-release. This specification covers ctl, setup, normalized events and version-info. Persisted task metadata, saved workflow definitions, workflow run documents, ownership receipts, follow-up ledgers, control phase payloads, live-input inbox rows, task artifact manifests and approval ledgers use qualified Harflow formats; store ownership and registered schema-preserving migration are defined below. Raw vendor streams, MCP negotiation, cursors and internal indexes are unchanged.

## Common envelope

Every wire document contains `product: "harflow"`, a format discriminator, integer `v: 1`, and `product_version: "0.0.5"`. Booleans are not integer versions. The formats are `harflow.ctl`, `harflow.setup`, `harflow.event` and `harflow.version-info`.

Validate identity and format before interpreting either a payload or an error. Missing or foreign product/format, unsupported versions and malformed product versions are rejected. Unknown optional fields and unknown normalized event kinds are additive. Payload fields cannot override the reserved common envelope fields.

Product versions are strict numeric `major.minor.patch`, with no leading zeroes. Each component is at most 2147483647. Optional dot-separated ASCII alphanumeric/hyphen build metadata after `+` is accepted and ignored for compatibility. Prereleases are unsupported. The complete version string is at most 256 bytes.

Live ctl, setup and version-info documents require matching major and minor components; patches in either direction are accepted. Validate each live response independently after the handshake. For persisted events, including events embedded in ctl pages, `product_version` is writer provenance: validate its syntax but do not require the current release line. Event product, format and integer wire version remain mandatory. A historical event from 1.0.7 remains readable by a consumer of event v1 running 1.1.0, while a live 1.0.7 engine fails that consumer's handshake.

## ctl and setup

`--json` emits one JSON document. ctl command-specific payloads and error objects retain their existing meanings, including batch `tasks`/`errors`. ctl usage errors contain `error.code: "usage"` and exit 2.

Additive resource reservation commands (no existing document changes shape; an older engine answers them with a usage error, which a consumer treats as unsupported):

```bash
harflow-ctl resources --json
harflow-ctl resources-release --reservation-id ID --confirm-uncertain [--also-release-session] --json
harflow-ctl resources-repair --json
harflow-ctl launch-settle --task-id ID --confirm-uncertain --json
```

A task that asked for resources has no task record until it spawns, so `harflow-ctl status` and `status-batch` answer its id with the additive error code `task_pending_launch`, carrying `launch_state` (`waiting_for_resources`, `granted` or `not_started`, plus `not_started_reason` once settled) in `error.data` (in `errors[id].data` for a batch). An older consumer sees an unknown error code and treats the task as unavailable; `list` and every existing document keep their shape. `harflow-ctl cancel` on a pending id answers the usual `result` with `cascade.pending_cancel_requested` and `pending_settled`. Spawned tasks gain an additive `resources` object (`names`, `reservation_id`, `state` `held` or `released`, `wait_seconds`, `wait_started_at`, `granted_at`, optional `released_at` and `borrowed_from`) on `status` snapshots; it is absent for a task that asked for none. `harflow-ctl run` gains `--resource` and `--resource-wait-seconds`.

Additive worktree commands: `harflow-ctl worktree-list --json` answers `worktrees: [{task_id, path, source_repo_path, base_ref, base_commit, branch, detached, state, exists, users: [{task_id, kind, status}]}]`, and `worktree-remove --task-id ID --json` (a verified person only; `agent_caller` or `caller_undecidable` otherwise) answers `result: {task_id, path, removed, branch, note}` or an error code `worktree_in_use`, `worktree_dirty`, `worktree_missing`, `worktree_busy`, `worktree_source_missing`, `worktree_unknown`, `worktree_link_invalid` (the tree's `.git` link does not lead back to the recorded source repository), `worktree_filters_unsupported` or `worktree_status_unavailable`. `harflow-ctl run` gains `--worktree`, `--worktree-base`, `--worktree-branch` and `--worktree-detach`; a pending launch's `task_pending_launch` `error.data` has the same `launch_state` values, which now also include `granted` for a worktree-only launch. Spawned tasks gain an additive `worktree` object (`path`, `source_repo_path`, `base_ref`, `base_commit`, `branch`, `detached`) on `status` snapshots; their `repo_path` is the worktree path. Absent for a task that asked for none.

Additive shared-scratch command: `harflow-ctl scratch list --json` answers `shared_scratch: [{kind, key, identity, path, bytes, bytes_truncated, last_used_at, last_task_activity, active_tasks, blockers, orphan}]` (`kind` is `chain` or `group`, `identity` the exact chain id or group string, `null` when the metadata is missing; `active_tasks` counts the tasks, launches and reserved continuations still using the folder, each once, and the additive `blockers` object `{active, unreadable, pinned, artifact}` counts every distinct reason the folder cannot be cleaned), and `scratch clean (--group NAME | --chain ID) --json` (a verified person only; `agent_caller` or `caller_undecidable` otherwise) answers `result: {kind, identity, path, bytes, bytes_truncated, deleted, note}` or an error code `shared_scratch_in_use`, `shared_scratch_unknown`, `shared_scratch_busy`, `shared_scratch_mismatch`, `shared_scratch_unsafe` or `invalid_chain_id`. Task records and `status` snapshots gain an additive `scratch_chain_id` (records only) and `scratch` object (`task`, `chain`, `group`, `chain_id`, `group_name`, `access` with `writable`, `no_additional_grant` or `sandbox_read_only` per path, `writable`, `notice`); both are absent for a record written before they existed. A launch receipt gains an optional `scratch_chain_id`. `scratch` also carries `system_guidance` (`state`: `delivered`, `reused_from_session` or `unavailable`, and `version`), and each backend's `capabilities` in `harflow-ctl backends --json` gains `system_guidance` (`first_turn_sticky`, `per_run` or `null`). See [shared scratch](shared-scratch.md). No fixture changes.

`resources` is read-only and creates nothing. Inside the ctl envelope it adds `resources`, one row per name that any reservation mentions, sorted by name: `{"name", "capacity", "holders": [...], "queue": [...]}`. Each row has `reservation_id`, `owner` (`kind` plus the owner's ids), `enqueued_at` and `generation`; holders add `granted_at`, and `borrows_from` or `release_requested` when they apply; queue rows add `deadline_at` when a wait has one. `resources-release` and `resources-repair` are for a verified person: they use the takeover gate and fail closed with `agent_caller` or `caller_undecidable`. `resources-release` answers `result: {reservation_id, outcome, also_release_session, session_released}` where `outcome` is `released` or `release_requested` (borrowers are still unresolved), or `unknown_reservation`; `--also-release-session` additionally settles the launch of a `granted` task that has no record (`session` is `released`, `not_applicable` or `unknown`; `session_released` is true only for `released`). `launch-settle` (a verified person only, same gate) settles a launch that was granted, left no task record and whose owner is confirmed dead: `result: {task_id, outcome, released_reservations, followups}` where `outcome` is `never_started` or `uncertain` and `followups` is the outcome written for the queued follow-ups it was handed (`released` only when the agent provably never started, else `delivery_unknown`), or an error code `unknown_launch`, `launch_has_record`, `launch_not_uncertain` or `launch_owner_not_dead`. `resources-repair` answers `result: {moved_to, rebuilt, restored, conflicts, note}`; `rebuilt` is true because it restores the holders it can prove from launch receipts and task records, with the borrowers that depend on them, and refuses a healthy ledger with `ledger_not_corrupt`. While a repair runs, or after one was interrupted, every resource operation reports `resource_ledger_repairing` (run `resources-repair` again to resume it). Every resource command reports a corrupt ledger as `resource_ledger_corrupt`. The ledger itself is a private format, `harflow.resource-ledger` v1, and adds nothing to the fixture bundle.

Setup results retain `server_path` and per-client `clients` rows. Client failures stay in those rows. With `--json`, process-level setup failures contain `error: {code: "setup_error", message: ...}` and exit 1; usage failures use `code: "usage"` and exit 2. Plain-text mode retains stderr and exit behavior. A nonzero process without a valid JSON envelope is a transport failure, not an empty successful result.

## Version-info and operation handshake

`harflow-ctl version-info --json` uses the thin lazy installed entrypoint. Success and invalid-usage paths import no ctl, task, backend, server, store or client runtime, read no configuration/history, probe no harness and create no files or directories, including with stale caller environment variables. Invalid usage emits a `harflow.version-info` envelope with `error.code: "usage"` and exits 2.

The payload has `component: "engine"`, `contracts` entries named `ctl`, `setup` and `events` with their exact `format`/integer `v`, and `capabilities: ["ctl.v1", "setup.v1", "events.v1", "version-info.v1", "state-recovery.v1", "caller-approvals.monitor-respond.v1"]`. `caller-approvals.monitor-respond.v1` qualifies Monitor-channel responses to caller-owned approval requests; a consumer must require it on the fresh handshake for that response operation. Missing required capabilities fail the corresponding operation; unknown additive capabilities are tolerated. `os` is Python's platform identifier (`darwin` or `linux`, for example); `architecture` reports the actual machine architecture. App integration requires `darwin`/`arm64`; reporting other platforms does not remove Linux engine support.

`state_root` is the selected absolute lexical engine root. An explicit supervisor `--root` or Python registry/store root takes precedence over `HARFLOW_STATE_ROOT`; otherwise the default is `Path.home()/.harflow`. A present override must be a nonempty absolute path without NUL characters. Relative explicit paths and relative HOME values are normalized against the process working directory; normalization collapses repeated separators, dot/dotdot components and trailing separators, preserves case and never resolves symlinks. The root need not exist. Compare roots case-sensitively; aliases that differ lexically refuse safely. Root selection and version-info remain independent of store ownership; version-info never acquires a lease or inspects the marker.

Consumers perform one fresh version-info probe immediately before each ordinary ctl or setup operation, with the same effective HOME, environment and working-directory policy. Setup probes the paired ctl from the selected runtime. Validate product line, required formats/capabilities, platform and selected state-root equality before launching the requested command. Historical ctl without version-info is incompatible. Approval is not cached across operations. This handshake does not establish authenticity or immutable runtime identity, and does not permit concurrent incompatible writers to share a store. Production managed runtime integration remains unavailable.

## Normalized events and budgets

Every persisted normalized event line and every projected event object retains the event envelope. Known and unknown additive event kinds share the same identity checks. Oversize replacement stubs also retain the envelope and writer provenance. Common discriminator bytes count toward the existing 16384-byte event and 262144-byte page budgets; limits are not increased. Raw vendor event logs are not rewritten or stamped.

## Canonical fixtures

[contracts/v1](../contracts/v1/manifest.json) is owned by the engine repository. Each fixture is canonical UTF-8 JSON or single-line JSONL with a trailing newline. `manifest.json` lists exact file SHA256 values and expected validation cases; `manifest.sha256` is the SHA256 of those exact manifest bytes. The fixture bundle digest is that manifest digest. Negative cases intentionally contain incompatible envelopes.

Swift vendors identical bytes in its test resources with provenance and the same digest. Each repository builds independently without sibling lookup or network fixture fetching. The fixtures include patch compatibility in both directions, historical inner event provenance, unknown fields/kinds and required capability failures. The bundle is compatibility test evidence, not release artifact pinning.

## Selected state layout

Public engine state uses `<state_root>/tasks`. Fresh and resumed agents and workflow supervisors inherit the actual selected root through `HARFLOW_STATE_ROOT`; vendor HOME configuration discovery is unchanged. Registry-owned workflow access uses that registry root. Default and legacy home state roots, the selected root and any independently valid ambient override remain protected from worker writable grants and artifact references.

Injected Python task directories with another basename remain available for in-process storage only; their selected parent root must be missing, empty or already compatibly marked before payload creation. Dispatch, continuation, detached launch and workflow reservation/supervision refuse those layouts before harness probes, subprocess creation or new task/run writes. The normalized basename must be exactly `tasks`, case sensitive, and its parent must equal the selected root. No private layout descriptor or additional CLI task-directory flag is supported.

Invalid selected roots produce the existing format-specific JSON `usage` error and exit 2 for ctl, setup and version-info. Plain text and stdio startup report the error on stderr and exit 2; no fallback root, successful `state_root`, registry or store is created. Help remains available.

## State ownership and recovery

An engine operation initializes a missing or empty selected root with `.harflow-store.json` and the stable `.harflow-store.lock`. The marker contains `product: "harflow"`, `format: "harflow.store"`, integer `v: 1`, integer `layout_version: 1`, and the current storage writer line in `product_version`. Same-line patches remain compatible in either direction. Private persisted formats additionally accept explicitly registered older stable writer lines; the current registry qualifies Harflow 0.0 schema 1 for all private formats listed below, including notification SQLite, task launch and observation documents. Numeric age does not establish compatibility. Live wire/runtime gates remain unchanged.

Crossing a registered storage line requires exclusive root ownership after all shared leases quiesce. A read-only preflight validates known authoritative private document product, format, schema and writer line, and the notification database identity/essential schema. It does not interpret raw vendor/event logs or derived indexes, normalize payloads, or replace the existing full semantic qualification on access. Unknown, foreign, malformed or newer authoritative envelopes refuse promotion. SQLite must have no nonempty sidecars; checkpoint it with the source engine before transitioning. The marker is then durably promoted before any new-line writes or shared ownership; immutable `origin_product_version` preserves its original provenance and unknown marker fields survive. The source-line engine rejects the promoted marker. Payload bytes are unchanged. Future structural schema conversion requires a separately qualified transformation; no schema guessing occurs.

The only automatic unmarked adoption is the exact set of four empty, regular, nonsymlink directories `workflows`, `workflow-runs`, `workflow-leases` and `workflow-owners`, with no other entries except recognized regular lock/publication infrastructure. Identity and emptiness are rechecked under exclusive ownership. A subset, superset, nonempty or symlinked set refuses. Other unmarked nonempty, foreign, malformed, unsupported-layout, unregistered or newer stores fail closed. Existing Polybridge state is never imported.

The marker, lock, `.harflow-store.json.pending` and recovery journal are reserved infrastructure. Do not delete or edit them to bypass a refusal. Compatible processes hold shared ownership throughout complete engine operations, detached task/scheduler lifetimes, executor teardown and retained state resources. Exclusive transitions cannot upgrade an existing same-process shared lease. Contention reports `state_busy`; ownership refusals preserve `state_incompatible` and exit 1. Optional `error.data` adds `reason`, `state_root`, `engine_version` and `recovery_supported`. Startup diagnostics do not certify recovery eligibility: inspect first.

### Explicit incompatible-state recovery

`harflow-ctl state-inspect --json` is read-only: it does not initialize, migrate, mkdir, create a lock or acquire a state lease. Its ctl `result` includes `state_root`, `engine_version`, `status` (`ready`, `missing`, `incompatible`, `interrupted`), stable `reason`, `message`, `recovery_supported` and `cooperative_ownership_only: true`. A trustworthy observed marker can add `observed_store`. Eligible incompatible roots add `recovery: {operation_id, confirmation_token, backup_path, phase, resumable}`. A prior-line marker must pass the same read-only promotion preflight before inspection reports ready; a document or database preflight refusal reports incompatibility without changing the marker. Ready/missing roots have no proposal. Reasons include `compatible`, `missing_root`, `unmarked_nonempty`, `foreign_store`, `invalid_marker`, `unsupported_layout`, `newer_store`, `unsupported_source_line`, `unsafe_root`, `infrastructure_unavailable` and `recovery_interrupted`. Inventory changes during inspection can report `stale_confirmation`.

Explicitly confirm the returned proposal using:

```sh
harflow-ctl state-recover --operation-id OPERATION_ID --confirmation-token TOKEN --json
```

The token binds the selected normalized root, root/lock identities, existing journal, recursive no-follow inventory, engine and operation. It is a stale-state guard, not authentication. Changed data requires a fresh inspection. Callers cannot select arbitrary backup destinations. Recovery rejects filesystem root, home itself, symlinked root/parents, special entries and unsafe infrastructure. It takes raw exclusive ownership even when the marker is incompatible. Known active or uncertain task, scheduler and controller process evidence, inner owner locks and same-process leases block recovery. There is no cancellation, process signaling or implicit resume.

The selected root inode and stable lock stay in place. A unique sibling `<root-name>.harflow-backup-<operation_id>` contains `data/` with original entries, `manifest.json` and a durable success `receipt.json`. An existing backup is never overwritten or deleted. A durable in-root `.harflow-recovery.json` records operation intent before creating the sibling or moving data. Original marker moves first; all remaining top-level entries then move by rename without traversing symlink targets. Journal, file and directory boundaries are fsynced. Every original entry remains in the root or backup on failure. Recovery inventories are bounded to 100,000 filesystem entries; oversized or unsafe inventories refuse. Backup is archival; no automatic import, restore or merge is promised.

Ordinary startup refuses unfinished recovery. Explicit same-operation retries validate both sides and resume the durable journal; substituted entries, missing/duplicate locations and unknown additions refuse. Qualified interrupted publication temporaries are reconciled only by explicit resume. Corrupt journal bytes cannot authorize a reset and require manual inspection. A damaged pending journal before its first atomic publication has not moved user data.

The fresh marker's `recovery_operation_id` is the irreversible commit point. After it is published, same-operation retries reconcile only receipt/journal metadata and never inventory, move or initialize newly authored data. This preserves writes by an older same-line participant between commit and response. A durable receipt answers response-loss repeats even after subsequent operations. A later distinct recovery archives an earlier completed journal byte-for-byte as `previous-recovery.json`; it does not replay the earlier reset.

Success returns `state_root`, `engine_version`, `operation_id`, `backup_id` (the operation ID), `backup_path`, `status: "completed"`, `phase: "completed"` and `recovery_supported`. Recovery failures preserve `code`/`message`; codes add `state_recovery_failed`, `state_recovery_stale`, `state_recovery_unsafe`, while `state_busy` remains contention/owner refusal. Data retains known operation/backup/path/phase evidence, including partial failures. Stable failure reasons include `stale_confirmation`, `active_owner`, `ambiguous_owner`, `backup_conflict`, `recovery_interrupted`, `unsafe_root` and `infrastructure_unavailable`.

The pure version-info capability `state-recovery.v1` advertises these commands. An older engine without it or returning usage is unsupported. Executable readiness remains independent of store readiness; an incompatible store does not disqualify a correctly identified executable. Ownership is cooperative: unsupported pre-ownership engines, external editors and vendor processes cannot be certified solely through the root flock. Recovery conservatively checks known process/lock evidence, but it cannot guarantee coordination with arbitrary nonparticipants. Selecting a separate fresh state root remains available.

## Persisted task metadata

Each engine-authored `tasks/<task_id>.meta.json` contains the flat reserved fields `product: "harflow"`, `format: "harflow.task"`, exact integer `v: 1`, and the writing engine's `product_version`. Private format compatibility follows the registered storage-line policy above; same-line patches work in both directions, build metadata is ignored and prereleases are refused. This is an engine-owned persisted contract; the companion app reads ctl projections rather than these files. Task metadata adds no cases to the cross-language wire fixture bundle.

Qualification precedes payload interpretation and full-prompt sidecar access. Required fields and every supplied known field, including process identity, enforcement, saved grants, lineage and outcome structures, must have their declared types. Booleans cannot stand in for integers. Unknown additive fields are tolerated; absent optional fields keep their defaults. Nested grant qualification is structural and does not inspect or reauthorize target files. Resuming saved writable grants separately requires the existing supported grant shape and current filesystem identity; additive metadata acceptance does not expand that permission contract. The source filename's task id must match the payload. Caller prompts and sidecar contents remain verbatim.

A verified absent metadata file is missing. Existing foreign, unqualified, malformed or incompatible metadata reports `task_incompatible`; ordinary I/O failures report `task_unavailable`. Ownership failures remain separate. Caller candidate acquisition retries one fresh whole scan only when every refusal positively identifies metadata changed during its read; it discards the first scan and requires the selected scan to be complete before filtering candidates. Ordinary acquisition uses one scan, and both caller-detection phases share the final candidates or failure. Direct reads and other complete-history consumers retain strict refusal without retry. Direct ctl errors and MCP error data retain these stable task codes. An unreadable full-prompt sidecar on otherwise qualified metadata retains the existing unavailable-assignment diagnostic: the preview is not authoritative and cannot be used as a replacement assignment.

Writers qualify an existing source before creating prompt sidecars or replacing metadata. Refused writes leave existing payloads unchanged and return the existing best-effort not-landed outcome with a task diagnostic. A later failed persistence attempt is distinct from any earlier success; it cannot authorize automatic continuation. An already started process or delivered signal remains reported as such, with persistence failure exposed separately. Cleanup, process settlement and event closure still run. Cooperating writers use the catalog lock and the target source identity is checked again immediately before replacement; an external editor that ignores locks can still race after that last check.

Lists may display compatible unrelated tasks while reporting `history_incomplete`, refused task ids/reasons or neutral `needs_direct_lookup` blockers. Task-catalog blockers use `incompatible_metadata` or `unavailable_metadata`, expose no caller/lineage/workflow authority, and invalidate stale task projections. Any blocker makes catalog-based caller authority globally unavailable. Bounded indexing exhaustion remains retryable preparation rather than incompatibility. Complete-history operations, including session reuse, retention and automatic follow-up reconciliation, fail closed when task history is incomplete. Cancellation cascades process known compatible descendants and report `cascade_incomplete` plus refused ids. Independent workflow-owned logical children may have receipts without standalone process metadata; a verified missing metadata file does not invalidate that independent workflow association, while an existing incompatible file is always refused.

The task-document qualification change intentionally refuses earlier unqualified development task documents (then labeled HarflowMCP 1.0.0), even inside a compatibly marked root. It does not bump `layout_version`, relabel, backfill or adopt those documents. Preserve the old root and select a fresh missing or empty root for ordinary development use. Identified files may be inspected or quarantined manually outside the engine; there is no automatic repair, deletion or legacy import. Explicit incompatible-state backup/reset is described above.

## Saved workflow definition documents

Saved files in `workflows/<name>.json` use an envelope with `product: "harflow"`, `format: "harflow.workflow.definition"`, exact integer `v: 1`, the creating engine's `product_version`, and an inner `definition` object. Compatibility follows the registered storage-line policy above; same-line patches and build metadata are compatible, while prereleases are refused. The inner name must exactly match the requested source name, including case. On a case-insensitive filesystem, a differently cased lookup that reaches an existing file is refused as a name mismatch rather than treated as missing. Known supplied fields and nested agent, reference, node and connection structures must have their declared types; unknown additive fields are retained. Qualification does not normalize graph semantics.

Authored save requests, returned definitions, dependency snapshots and graph hashes continue to use the inner graph payload. Only the store creates persistence envelopes. Existing flat development definitions, foreign or malformed documents and unsupported versions produce `workflow_definition_incompatible`. Unreadable, replaced, disappeared or symlinked sources produce `workflow_definition_unavailable`; a verified absent requested definition retains the ordinary missing-reference behavior. MCP errors include structured `name`, `code` and `reason`; ctl retains the stable refusal code.

Listing fails as a whole if any discovered source is refused. Dependency resolution indexes the complete store, so an unrelated refused definition also blocks a save requiring that index. Delete qualifies its target and completes the qualified referrer scan before removing it; refused sources are never deleted through ordinary engine operations. Save and delete recheck source identity under cooperative tree and definition locks, including absence for new saves. These checks detect observable changes, without claiming a filesystem transaction against an uncooperative writer.

Explicit delegation graph migration reads qualified definitions and writes qualified replacements. Its `workflow-backups` snapshots use the same envelope and retain the projected pre-migration payload; they are backups, not implicitly adopted definitions. The migration command does not adopt raw legacy files. Preserve refused files for explicit manual inspection or quarantine outside the engine, or select a fresh state root.

## Workflow run documents

Files in `workflow-runs/<workflow_run_id>.json` contain `product: "harflow"`, `format: "harflow.workflow.run"`, exact integer `v: 1`, the writer's `product_version`, and an inner `run` object. The registered storage-line policy above applies; same-line patches and build metadata are compatible, while prereleases are refused. The inner run ID must exactly match the filename before it can select locks, lineage, sessions, ownership or controls. Public get, status and list results continue to expose the inner run payload.

Qualification checks the common identity and traversal containers and every supplied known authority field, including process identity, nested attempts, native settlement flags, parent links, session bindings, grants, recovery, goals and notification points. Booleans are not integers and numeric values must be finite. Unknown additive JSON fields survive. Embedded authored definitions, builder drafts, candidate/config snapshots and graph hashes retain their existing meaning; reading does not migrate graphs or apply current authoring budgets to historical goal history. Embedded caller records use task payload qualification, while native attempts and stored result snapshots remain their own shapes. Existing permission, relationship, nonce, digest and backend certification checks still apply separately.

Existing flat development runs, foreign or malformed documents, mismatched IDs and unsupported versions report `workflow_run_incompatible`. Unreadable, nonregular, symlinked, replaced or disappeared sources report `workflow_run_unavailable`; only verified initial absence is missing. Bounded read exhaustion retains its existing retry/preparation behavior. CLI and MCP errors carry the stable code and run diagnostic. No reader repairs, relabels or adopts a refused file.

Audited unlocked control, supervisor liveness, child-watch/input and direct status/detail observations may refresh once after a stable descriptor is superseded by a different device/inode. Superseded bytes are discarded before decoding; the replacement must independently pass every qualification and final descriptor/path check. Both reads consume the same existing request budget, so a tight budget may refuse the second read. Missing, symlinked, nonregular, in-place-mutated or repeatedly replaced sources still refuse. Locked authority reads remain strict. Refresh is opt-in: unlisted native, on-demand, builder and other observations remain strict; this is not a guarantee that all concurrent reads refresh.

Complete run inventories fail closed if any discovered run is refused. One refused run therefore blocks inventory-based negative ownership claims, retention and delegation migration even for otherwise unrelated IDs. Bounded display remains available with neutral catalog blockers that expose no trusted session, lineage, status or checkout ownership. Run projections have an independent generation; task projections retain their existing format. Retention preserves evidence when qualification or ownership is incomplete.

Writers qualify the complete candidate before invalidating catalogs or publishing run bytes, receipts or journal entries, and recheck the physical source immediately before replacement. Invoked children retain exclusive no-clobber publication. These are ordered individual writes, not a multi-file transaction: a run can be published before a later receipt, journal, directory durability or index operation fails. Such failures identify the run as `published: true` rather than implying it did not land; recovery must inspect the qualified durable run and retained evidence. Ownership failures retain their ownership error code and publication evidence. Cooperating locks and identity checks do not eliminate races from external writers that ignore them.

This unreleased change refuses older unqualified development run files without changing the store layout version. Preserve the old root and select a separate fresh missing or empty root. Journals, inboxes and notification databases are not migrated by this run-document contract.

## Detached task ownership

`tasks/<task_id>.launch.json` is private engine state with `product: "harflow"`,
`format: "harflow.task-launch"`, exact integer `v: 1`, a compatible stable
`product_version`, filename-bound `task_id`, selected lexical `root`, task-directory
device/inode identity and a random launch `generation`. It records request and
configuration hashes, backend/session/ancestry identity, owner identity and the
launch state (`reserved`, `claimed`, `spawned`, `settled` or `not_started`).
The permanent `<task_id>.launch.lock` serializes claiming and execution. Reads
are bounded to 64 KiB and refuse unqualified or changed sources. Unresolved
reservations participate in session exclusion, successor history and retention.

The reservation is not executable authority. The owner accepts a prepared request
only from its private inherited descriptor, checks its hash/generation/root, and
repeats backend and saved-grant qualification. The private transfer is bounded to
4 MiB and preserves initial stdin bytes losslessly. An identical proven
pre-execution retry retains the reserved session; changed settings or uncertain
execution refuse reuse. No stored launch file is automatically replayed.

Native transport observations use `tasks/<task_id>.observations.jsonl` and
`<task_id>.observations-final.json`. Each document carries the private
`harflow.task-observations` format with the same Harflow version qualification,
task identity and launch generation. Rows have contiguous integer sequences and
raw correlated events. The final document carries the exact terminal watermark
and ledger byte count. Readers return bounded pages and must reach that watermark
before workflow child settlement, fallback, joins or completion. Missing or
incompatible observations do not establish that no children ran. These records
do not create public event kinds or replace native adapter qualification.

Task-owner death preserves uncertainty and captured evidence. It does not
authorize another execution or fabricate observed exit/drain evidence. These
private formats require no cross-language fixture changes; the application
continues consuming public ctl projections. See [supervision and recovery](supervision.md).

## Workflow ownership receipts

`workflow-owners/<task_id>.json` uses `product: "harflow"`, `format: "harflow.workflow.receipt"`, exact integer `v: 1`, a registered compatible storage-line `product_version`, the filename-bound outer `task_id`, and a nested `receipt` object. Public receipt reads return that inner object unchanged. Its required `workflow_run_id` is an identifier; optional root/session-owner run IDs are identifiers or null. Supplied node/execution IDs, role, name, status, execution contract and interaction owner are strings or null; additive finite JSON fields and omitted optional fields are preserved. Native logical tasks may have receipts without standalone task metadata. Receipts do not replace independent run membership, activation, role, root or live-attempt checks.

Receipt reads are bounded to 16 KiB and reject symlinks, nonregular files and observable source changes. Only verified initial absence permits existing catalog or ancestor fallbacks. Existing unqualified, malformed or incompatible receipts report `workflow_receipt_incompatible`; unreadable or changed sources report `workflow_receipt_unavailable`. Read-budget and state-ownership failures remain distinct. Display consumers expose incomplete ownership/history and suppress refused labels; refusal never grants standalone artifact or caller authority.

Run mutations encode all resulting receipts and qualify existing targets before catalog invalidation or run publication, then recheck each observed target immediately before replacement. A refused target is not overwritten. Repeated targets retain the last payload. Durable writes remain ordered run → receipts → journal → index, without a cross-file transaction or rollback. A later receipt failure retains its cause in a run error with `published: true`; earlier receipts may already have landed. Cooperative locks and source checks cannot eliminate the last-check race with nonparticipating writers.

A refused retained receipt also blocks cancellation, terminal and supervisor-stop recording. The durable run may retain its prior status and a stale supervisor PID. A supervisor invocation reports the refusal and suppresses automatic relaunch only for its own blocked run, including receipt preflight read-budget exhaustion; child refusal does not suppress a healthy parent's eligible relaunch. Preserve the old root and select a separate fresh missing or empty root for recovery. No receipt adoption, repair or schema conversion is performed; explicit incompatible-state backup/reset is described above.


## Queued follow-up ledgers

`tasks/<task_id>.followups.json` uses `product: "harflow"`, `format: "harflow.task.followups"`, exact integer `v: 1`, a registered compatible storage-line `product_version`, the filename-bound `task_id`, and nested `ledger`. The existing inner ledger shape, queued responses, composition order and caller prose remain unchanged. Unknown finite JSON fields and omitted optional fields survive qualification; final-item compaction still retains metadata only.

Reads reject unqualified, foreign, incompatible, malformed, symlinked, nonregular, unreadable, oversized and observably changed sources. Only verified initial absence means an empty queue. Refused sources report the existing unavailable view and unknown count, block consuming continuation before launch, refuse queue mutations and protect retention. Readers never retry or repair the source. Even a concurrent atomic replacement may transiently report unavailable.

Every mutation holds the permanent follow-up lock, qualifies the candidate, and checks its captured source identity or absence immediately before replacement. Admission's 960 KiB and internal/read 1 MiB limits count the entire serialized envelope. Terminal bookkeeping failure retains the existing launch outcome and process-local retry obligation; it cannot authorize overwrite of a refused source. Retention rechecks the source before granting deletion permission and keeps the permanent lock inode. These cooperative checks do not eliminate the last-check race with external writers that ignore locks.

Preserve refused files and the old state root for manual inspection outside the engine, then select a separate fresh missing or empty root for recovery. No adoption, historical conversion, or changes to inbox, control, notifications or other persisted families are provided by this ledger contract.

## Control phase payloads

`tasks/<task_id>.<family>.<attempt>.<phase>` carries a private persisted envelope with `product: "harflow"`, `format: "harflow.task.control"`, exact integer `v: 1`, registered compatible storage-line `product_version`, explicit filename-bound `task_id`, `family`, `attempt` and `phase`, and nested `payload`. Safe generic family and phase names remain supported; attempts are positive exact integers. Discovery keeps its existing attempt-number limits. Supplied known payload fields receive structural checks, without adding required fields or process-identity authority. Optional null session/start-time values and additive finite JSON data remain valid. This format is not a public wire capability.

Parsed reads return absence only after verifying the path is missing. Unqualified, foreign, incompatible, malformed, unreadable, symlinked, nonregular or observably changed sources remain undecidable. Reads do not retry or impose a new byte budget. Qualification does not establish process death or authorize signalling, lease recovery or attach release.

Every candidate is fully qualified and encoded before temporary-file creation. Publication retains fsync and once-only hardlink semantics: an existing phase is never overwritten, adopted, repaired or relabelled. Candidate failures raise `PhaseWriteError` before an intent can authorize signalling; ownership errors remain separate. Existing bounded inline phase-write retries and unbounded background phase retries are unchanged.

Filename-presence latches remain a separate protocol: sig outranks failed, nosig resolves its join, failed takeover releases its reservation, and unreadable ready remains conservatively present. Permanent lock files and lease, signal and retention rules are unchanged. Parsed payload qualification does not qualify these presence latches or change their physical symlink policy.

Older flat development req/join documents remain conservatively pending and protect retention; flat takeover req/attach documents retain busy reservations, and flat sig payloads cannot authorize leader cancellation. Preserve the old root for manual inspection and select a separate fresh missing or empty compatible root. No legacy import, payload rewrite or automatic repair is provided; explicit incompatible-state backup/reset is described above.

## Live-input inbox rows

Each persisted row in `tasks/<task_id>.inbox.jsonl` uses the private `product: "harflow"`, `format: "harflow.task.inbox"`, exact integer `v: 1`, registered compatible storage-line `product_version`, filename-bound `task_id`, `kind: "message"` or `"seal"`, and nested `payload` envelope. Message payloads require string `id` and `text`; optional fields are not required or restricted beyond finite JSON. Seals require exact `closed: true` and no `id`. The discriminator must agree with the payload. Readers return the original inner message shape and preserve caller prose. Writers retain existing `default=str` coercion before qualifying the resulting JSON; nonfinite values are refused.

Qualification is row-local: unqualified flat development rows, foreign or incompatible envelopes, malformed JSON and invalid UTF-8 complete rows are skipped while byte offsets advance. Readers leave incomplete trailing bytes unread. Closure detection separately recognizes a qualified parseable trailing seal without a final newline; qualified seals never become messages. The physical `.inbox.closed` marker remains an existence latch regardless of its incidental contents.

Writers fully qualify and encode the UTF-8 candidate before opening the append target or terminating a torn tail. Candidate failures use the existing write-failure path and do not change existing bytes. Durable append, sender marker recheck, queued acknowledgements and delivery-event evidence retain their existing meanings. The inbox inode remains the permanent flock identity: no replacement, truncation, reset, historical rewrite or automatic migration occurs. Row qualification does not bind reopened paths to lock descriptors against external substitution or introduce a delivery transaction, retry or recovery guarantee. Pending display qualifies only inbox rows; independently parsed delivery events retain their own format and incomplete-evidence suppression.

Preserve old state for inspection and select a separate missing or empty compatible root for recovery. This unreleased private format is not a public wire capability and does not change the store layout version.

## Task artifact manifests

`tasks/<task_id>.artifacts.json` uses a flat private header with `product: "harflow"`, `format: "harflow.task.artifacts"`, exact integer `v: 1`, registered compatible storage-line `product_version`, filename-bound `task_id`, and the existing `items` array. The full document must contain finite JSON, including additive outer fields. Item, provenance and workflow-binding checks remain independent of this header; qualification grants no caller or path authority. This format is not a public wire capability.

Missing manifests remain empty registration sets. Existing unqualified development manifests, foreign or incompatible headers, malformed documents and unreadable sources report unavailable and cannot be overwritten by registration. No adoption, conversion or repair is provided; preserve old state and select a separate fresh missing or empty compatible root.

The complete persisted bytes, including the header, count toward the existing 1 MiB manifest cap and shared metadata budgets and determine task cursor revisions. Reads retain no-follow regular-file checks; registration retains the existing task/catalog locks and owned-directory atomic publication. Duplicate immutable references, workflow selection and stat-only target observations are unchanged. Manifest qualification does not read, copy or hash referenced file contents.

## Task approval ledgers

`tasks/<task_id>.approvals.json` uses a flat private header with `product: "harflow"`, `format: "harflow.task.approvals"`, exact integer `version: 1`, registered compatible storage-line `product_version`, filename-bound `task_id`, and the existing `requests` array. Every request must carry that same task identity and its existing canonical action digest. The digest covers the action alone. The entire ledger, including additive fields, must be finite JSON. This private format is not a public wire capability and does not change the store layout version.

Verified absence returns an empty qualified ledger in memory without creating a file. Unqualified development ledgers, foreign or incompatible headers, task mismatches, malformed documents, symlinks, nonregular files and unreadable sources are refused before display or decision use. Standalone task summaries report approval uncertainty on unreadable state; workflow aggregation reports read diagnostics separately from verified approval interactions. Refusal cannot reserve a decision or authorize stdin delivery. Preserve old state and select a separate fresh missing or empty compatible root; no adoption, import, rewrite or automatic repair is provided.

Reads use bounded no-follow regular-file descriptors and accept complete generations across ordinary atomic replacement. The complete header counts toward the existing 2 MiB ledger cap and 256 KiB decision/outcome headroom. Writers retain the permanent ledger lock, locked reread and atomic publication; candidates are fully qualified and canonicalized before temporary-file creation. No lock is held across task/workflow persistence or awaits.

Ledger identity is an audit qualification, not reusable permission or proof of execution. Existing owner, workflow, process, session, digest, revision, generation and deadline gates remain independent. The owning stdin pump still persists reservation and sending state before input; uncertain delivery cannot be replayed. Delivery-time path revalidation and protected-target checks remain in force: if an approved path no longer matches its admission snapshot or has become protected, the pump records the caller's decision but converts its effective decision to rejection before delivery. Private path snapshots are not part of public projections. A human decision, a reserved/sending/sent delivery, and an observed execution outcome are distinct facts.

The ctl approval projections are additive within `harflow.ctl` v1. `decided_by` retains its existing meaning: `caller` for a human decision, including a response accepted through Monitor, and `harflow` for a system decision. Optional `responder` records the channel that supplied a human answer (`caller` or `monitor`) and is derived from the authorizing ctl channel; it is not a caller-provided role. Older rows may omit it. MCP always acts through the caller channel and cannot assert Monitor authority. The optional `decision_reason` and `decision_reason_type` retain the backend-provided reason and its type, including the existing `null` representation; text is untrusted, bounded to 1 KiB, excluded from the action digest, and is not duplicated in a new reason field.

`approval_summary.pending_count` and `interaction_state` keep their established meanings. The additive `awaiting_decision_count` counts only requests whose disposition and delivery are both pending, with no human decision and no system rejection. It therefore excludes answered-but-reserved rows and other pending-delivery states. This count is for discovering requests that still need a decision; it does not certify that a decision can be applied when submitted. Older engines and previously persisted workflow documents may omit it.

Workflow `approval_summary` may add `read_status: "incomplete"`, `read_errors`,
`read_error_count` and `read_errors_truncated` when a source could not be read. Each diagnostic
identifies `task_id` (or `workflow_run_id` for traversal failure), `error_type` and a bounded
`message`; at most 32 diagnostics are returned. These fields describe incomplete observation,
not a request for caller attention, and do not authorize a decision or replay. An unreadable
source alone leaves `interaction_state` null; readable pending requests or genuinely uncertain
approval ownership retain their existing interaction state. Missing ledgers remain empty.
Workflow aggregation retains its 8 MiB source budget and the per-ledger byte bound.

Approval responses retain the outer `error.code: "approval_error"` and may include a typed `error.data.reason`. Known reasons are `not_current`, `already_answered`, `settled`, `expired`, `ownership_lost`, `unauthorized`, `unsupported`, `caller_undecidable` and reserved `incomplete`. Response checks have this precedence: authorization (`unauthorized`, `caller_undecidable`, `unsupported`), task/context lookup (`not_current` when the task is absent), row-owner/channel authorization, then request/digest/revision/workflow identity (`not_current`), then current row state (`settled`, `expired`, `already_answered`). The engine then reconciles the task; if reconciliation attempts invalidation, it returns `ownership_lost`. Otherwise it checks owner liveness and generation, then attempts the reservation; the reservation repeats identity and state checks under the ledger lock to arbitrate competing responders. A competitor submitting the original revision loses with `not_current`; `already_answered` applies when the submitted identity is current but a decision already exists. Every typed refusal other than `ownership_lost` guarantees this call did not change the ledger. Another concurrent process may still change it. `ownership_lost` guarantees no new human decision was applied, but reconciliation may have invalidated rows. Consumers must treat missing or unknown reasons as non-actionable and refresh read-only without resubmitting. In particular, ledger lock contention retains an untyped `approval_error`; it does not grant an unchanged-result guarantee. The ctl-wide mutation guard may instead return the existing top-level `caller_undecidable` error. A transport failure or timeout after submission leaves the result uncertain: refresh using read operations and do not resubmit.

The checked-in ctl examples are fixtures captured from live ctl output, with deterministic redaction applied to incidental identifiers, timestamps and digests; action bytes and their canonical digests remain consistent after redaction. They document the provenance and shape of actual responses. Synthetic compatibility cases such as omitted optional fields, legacy errors without `data.reason`, unknown reasons, or reserved typed reasons are identified as compatibility coverage, not claimed as live engine output. Fixtures never expose private admission path snapshots.

Regenerate approval examples with `uv run python scripts/generate-approval-fixtures.py`; `--check` compares two fresh isolated runs with the checked-in bundle. The generator seeds task state, clocks, process identity and pipe writes without a model call, then captures actual ctl commands. It does not certify a live harness. `--provenance PATH` writes the per-fixture source mapping. Synthetic cases have a `-synthetic.json` suffix. Incidental legacy environment aliases in diagnostics are replaced with explicit redaction placeholders and identified in the per-fixture transformation metadata.


## Notification database

Task and root-workflow subscriptions share `notifications.sqlite3`. Its singleton
`harflow_notification_identity` row contains `singleton: 1`, `product: "harflow"`,
`format: "harflow.notifications"`, native SQLite integer `schema_version: 1`, and
the creating engine's registered compatible storage-line `product_version`. Compatible
patch reopens preserve the creating version. SQLite integer storage cannot retain
whether a bound integer originally came from a Python boolean.

Qualification requires this identity and all three current tables: `subscriptions`,
`workflow_subscriptions` and `workflow_deliveries`. Essential column affinities,
nullability, primary keys, literal defaults and unconditional BINARY unique keys
are checked before application writes. Harmless additive columns and indexes are
accepted; missing tables or constraints are not repaired. Existing unqualified,
legacy or incompatible databases are refused with `NotificationDatabaseError`,
a `ValueError`; actual state ownership failures remain separate. Registered storage-line
compatibility does not repair database schemas or add row envelopes. Explicit root backup/reset
is described above.

Only a verified missing or zero-byte regular main file can bootstrap. SQLite must
also report empty schema and zero application/user versions under `BEGIN IMMEDIATE`,
unless another initializer has already committed a qualified database. Identity
and all three tables commit together before the connection is returned. Existing
header-bearing schema-empty SQLite files are not fresh. Status, unsubscribe and
retention pin inspection treat absent or zero-byte storage as empty without opening
SQLite; they never initialize it. Invalid existing storage is a refusal, including
when a required workflow table is absent. Retention must skip deletion when pins
cannot be established.

Harflow creates rollback/DELETE-journal databases. Any observed `-wal`/`-shm` path
or WAL main-header format is refused before SQLite opens; symlink/nonregular main
and journal paths are refused. Ordinary regular `-journal` files are permitted,
and SQLite manages locking and rollback. Read-only status never repairs, changes
journal mode or chmods storage. Writable existing opens first qualify read-only,
then qualify their actual writable connection before application PRAGMA, chmod or
row changes. Normal refusals preserve existing bytes and permissions.

The narrow physical exception is native crash recovery: only an actual extended
`SQLITE_READONLY_ROLLBACK` (776) from the first read-only qualification query allows
a writable operation to open the database for SQLite rollback. The restored state
must then qualify before Harflow writes. This may change the crashed main file or
remove its journal even if the restored database is foreign and is refused; no
Harflow schema or metadata is added to that foreign database. Read-only status
fails until a writable operation recovers it. Other readonly, busy and corruption
errors do not authorize this exception. These checks do not form a transaction
against independent external path replacement or certify external WAL writers.

### Supervision and recovery activity

Task snapshots add `supervision` with independent tri-state `owner_state` and
`process_state` (`alive`, `dead`, `undecidable`). `output_capture` aliases its
`capture_state`: `live`, `lost_with_owner`, `unknown`, or `observed_terminal`.
`input_channel` aliases `live_input_state`: `open`, `closed`, `closed_with_owner`,
`unknown`, or `not_applicable`. Confirmed owner death alone supports the loss
claim; conservative process guards do not establish verified liveness. `live_input` is the
invocation capability, not a current acceptance claim. Optional `input_closed_reason` and
`supervision.live_input_closed_reason` name a known sealed-input reason and are omitted when unavailable.
Input closure can precede process exit; only observed settlement ends the running state. Observed
terminal history retains its observed status. `last_captured_output_at` is the
raw stream file modification time in UTC, or null when unavailable; supervision
labels its source `raw_stream_log_mtime`.

`recovery-activity TASK_ID --json [--cursor CURSOR] [--limit N]` is an additive
ctl command. Its `recovery_activity` page carries `source: backend_transcript`,
`display_only: true`, `events`, `next_cursor`, `reset`, `has_more: false`, and
bounded `notices`. Limits are 1..100. Events have independent stable `source_id`
and `source_ts`, with ordinary assistant_text/tool_call/tool_result payloads;
tool call identities use a `recovery:` prefix. These events never receive task
log sequence numbers and are never written into a task log. Poll the independent
cursor to fetch later appends; a replaced, truncated, or evicted tail resets the
cursor explicitly. Pages omit older activity beyond the bounded tail/page.
Status snapshots and MCP get_task_events expose the same additive recovery page;
MCP recent_activity includes a bounded display projection.

Transcript recovery is available only after confirmed capture loss, only for a
backend with a qualified reader, and only for the exact persisted session.
It never affects execution classification, usage, outcomes, cancellation or
settlement. Claude searches at most 512 immediate project directories, reads
at most 256 KiB from one no-follow regular exact-session file, ignores partial
JSONL lines, and filters against task start and capture cutoff. Missing or
ambiguous source data yields notices. The raw-log timestamp cutoff is an
approximation; absent cutoff data is explicitly disclosed.

Task records and snapshots add effective `background_idle_seconds` (finite,
positive, at most 21600). Old records without it use the current environment
default or 600 seconds. The new diagnostic category
`background_abandoned_at_idle_bound` records the bound, actual idle waiting
elapsed time, bounded background descriptions and separate runtimes, latest
result subtype/error/summary and `successful_agent_result`. Earlier failure
diagnostics take precedence. Ordinary forced close does not claim idle cutoff.
All additions retain wire v1 and are optional for older consumers.

### Acknowledged workflow context sections

The public document and wire versions remain 1. Optional node `purpose` is preserved in
workflow definitions, including the patch compatibility fixture. Internal prompt receipts
use version 2 with `section_state` metadata keyed by known context section: `digest`,
`revision`, `mode` (`full`, `reference`, `append`), UTF-8 `source_bytes`, `source_characters`, and list
`length` where applicable. `section_delivery` exposes the same accounting metadata with `delivered_bytes` and
`delivered_characters`; generic section `bytes`/`characters` retain actual injected sizes.
No checkpoint prose is duplicated in this state. Older internal receipts cannot authorize
version 2 references. The workflow-run patch fixture covers these additive fields.
Instruction inspection is an internal orchestrator action over the current run's pinned
definition; it adds no ctl or MCP command and retains the existing inspection limits.

### Runner-owned structural transitions

Optional connected-node `always_route` is a boolean in workflow definition v1. The
patch definition fixture covers it alongside `purpose`. Guided run decision records
may have `actor: "harflow"`, `token_id`, `selected_connections` and
`transition_sequence` with a stable `decision_id` and reason, without `activation_id`.
Runner reasons begin with "Harflow advanced" or "Harflow passed" so consumers displaying
only reason text retain attribution. Optional `connections` is a string-array compatibility
alias of `selected_connections` for existing canvas consumers. These are runner decisions
rather than harness turns. Tokens may carry
`structural_completion_refs` contributed by skipped successful exits, merged at their own
join and retained only along structural edges; these references preserve existing
checkpoint-owned checklist evidence without completing tasks implicitly. Run history transition entries
retain their existing `detail` list of connection IDs and may add top-level `actor`,
`decision_id`, `reason` and `token_id` matched to the committed transition sequence.
The public wire versions remain 1; these fields are additive. The run patch fixture
covers the runner decision provenance.

### Additive permission and continuation projections

#### Publication permission policy

`publish_attempts_allowed` records caller intent to permit publication attempts. It does not
guarantee command approval, credentials, remote access or publication success. Backend policy
remains visible through `enforcement.mechanism` and `enforcement.caveats` in task records and
permission previews.

Claude's built-in `publish` allow-list grants `git commit`, `git push`, `gh pr create`,
`gh pr edit`, `gh pr view`, `gh pr comment`, `gh pr review`, `gh pr checks`, `gh pr diff`,
`gh pr list` and `gh repo view`, including arguments and `--help`. These built-in grants
do not depend on dispatch-time `allowed_commands` qualification. They do not include
`gh pr merge`, `gh repo delete`, `gh release`, or raw `gh api`. Claude's prefix patterns
cannot ensure GET-only API access: appended method flags or fields can turn an apparent
read into a write. Use the supported PR/repository commands with `--json` for reads.
No excluded command is implicitly granted by this list; inherited harness settings may
grant additional authority, and shell wrappers are not confined by an OS Git sandbox.

Other backends do not implement this command allow-list. Codex enables network within its
workspace sandbox and does not filter GitHub subcommands. opencode retains the `build`
policy shared with `write_in_repo`, so inherited permissions can still require approvals.
Vibe and Antigravity collapse `publish` into their unrestricted approval mechanism and
cannot separately exclude merge, deletion, release or API writes. Callers requiring those
exclusions must select a backend/policy that can express them; the freedom name alone does
not enforce them. Network restrictions remain independently reported.

Enforcement adds `publish_attempts_allowed`, retaining
`publish_attempts_allowed_by_polybridge` as a deprecated, identical alias for one release.
Legacy-only records remain readable. Contradictory dual values are invalid, including during
nested permission comparisons. This is an authorization disclosure, not a publication guarantee.
No canonical fixture shapes are changed by these optional additions.

MCP `get_task_status` includes a `successor_tasks` detail reference by default. Explicit
`include_successors: true` adds up to 20 retained authorized task IDs, `history_incomplete`
and `has_more`. The listing accepts
`predecessor_task_id` with its scope-bound cursor. It selects direct `parent_task_id` resumes and
`continuation_of_task_id` continuations, never unrelated same-session records. Missing retained
history cannot be reconstructed. Existing successor records keep their source IDs; predecessor
owner metadata is not rewritten. Subscriptions remain attached to the original task ID.

The 0.0.3 producer extends private artifact provenance with
`{method: "observed_backend_plan", task_id, tool_use_id}` for engine-observed successful plan writes.
The reader accepts both this variant and existing `verified_process_lineage` references, validating
the variant strictly. Older engine implementations reject manifests containing the new variant as
unavailable; downgrade readers cannot inspect those manifests, and must not rewrite them. Use the
updated engine to read them. This private-format extension is distinct from additive status fields.
The public artifact page retains its existing structure, adding a registrar method string and its
evidence ID. The Mac artifact decoder accepts registrar strings and extra provenance fields, so no
Mac fixture or version-gate update is required; this plan-provenance extension itself does not alter canonical fixtures.

### Observer identity and Git diagnostics

Task status, brief and list projections add bounded `dispatch_origin`, `responding_server`,
`observation_mode` and `observation_note`. `owner` retains its execution-owner meaning. The
optional private task field `dispatch_origin` is qualified like a process identity and is carried
through the private launch transfer. Older records with no origin remain readable and report
unknown observation continuity. These diagnostic fields do not authorize any state mutation.
`recovered` now reflects the observer relationship, with conservative true for unknown identity;
see [supervision](supervision.md#dispatch-origin-owner-and-responding-server).

Outcome baseline/terminal objects add optional `observation_status` and bounded `diagnostics`;
the outcome root adds capture diagnostics. Each diagnostic uses a phase, millisecond budgets and
timings, and at most 16 probe entries with bounded labels/categories and optional numeric byte
counts/return codes. Outer baseline interruption can retain partial evidence and name its outer
status/budget.

The existing `new_commits` field means newly reachable commits from initial HEAD, including
history imported by checkout or fast-forward; task-created commits and authorship are unavailable.
`base_commit` remains the original baseline and the range algorithm is unchanged. Full response
attribution clarifies these semantics for legacy records without rewriting saved observations.
Brief and notification outcomes add bounded `baseline`/`terminal` endpoint projections (`head`,
`branch`, `provenance`) and disclose shortened commit lists with `new_commits_truncated`.
Endpoints cannot reveal intermediate branch transitions, even when equal. This is an existing-field
semantic clarification and additive v1 projection; canonical fixtures contain neither `new_commits`
nor `committed_diff_stat`, so the fixture bundle and consumer version gates remain unchanged.

These are additive v1 response fields; decoders and version gates remain unchanged. Canonical
approval-status fixtures include the observer projections, using an isolated deterministic
responding identity, and the Mac repository vendors the byte-identical complete bundle. Engine
fixtures are generated first; consumer decoding is then verified against the new manifest digest.

### Review follow-ups: detail projections and plan provenance

`get_task_status` adds `include_successors` (default false). Default `successor_tasks` contains
`state: "not_requested"` and the existing `list_task_page` detail reference, without an ID array or
completeness claim. Explicit inclusion returns `state: "loaded"` and the authorized first page,
including pagination/incompleteness fields. Compact status preserves that reference and opt-in
behavior. Ordinary status polling performs no successor catalog query; explicit page reads retain
all existing authority checks. This changes the default of the unreleased MCP-only projection;
ctl snapshots and canonical fixtures retain their structure.

Optional detail `supervision.dispatch_origin_state` reports qualified dispatcher liveness separately
from observer mode; it is omitted for records with no origin. It is diagnostic, never authority.
Private `observed_backend_plan` provenance accepts legacy three-key references and a strict new
variant adding `plan_directory` plus `{device, inode}` in `plan_directory_identity`. Only new
references can establish availability independently of the reader's environment; legacy references
remain readable but unavailable. No canonical fixture requires a shape change for these optional
fields, and existing Mac decoders ignore unknown provenance members.


### Workflow resources and worktrees

Saved workflow definitions accept additive optional top-level `resources` and agent-node
`resources`, including on-demand nodes. New run fields `resources`, `resource_scope`, `resource_anchor_names`,
`resource_wait`, `worktree` and `source_repo_path` are codec OPTIONAL. Activations optionally carry
`resource_attempt`, `resource_wait` and `resource_held_waiting_for_slot`; task reservations retain
their candidate's resource-attempt receipt. These additions introduce no task status or wire
version, and do not change the fixtures under `contracts/v1`.

`start_workflow` and `preview_workflow_run` accept an optional worktree object using the task
worktree options. Preview hashes bind the resource declarations and worktree plan. Compact and
Monitor responses expose bounded resource waits and worktree metadata; paged views retain full
content. Resources grant no permissions. Existing managed-agent dispatch authority remains
unchanged; an independent workflow launched by a standalone holding task must borrow all names
from that scope. Linked child runs share the root worktree and enclosing resource coverage.

Worktree inventory rows for root workflow checkouts use optional `workflow_run_id` instead of
`task_id`; existing task rows retain `task_id`. Usage rows likewise identify either a task or a
workflow run. Human-only removal accepts either recorded owner ID. These additive optional
fields do not change the version-one fixture bundle.

Compact workflow status keeps optional `resource_wait` as a run-level object and adds
`resource_waits`, an array of at most eight active activation waits. Each row includes an
`execution_id` and resource attempt identity, names, state, reservation ID and bounded blockers.
The `resource_waits` detail view and Monitor snapshot retain all active waits; Monitor digests
track that derived view. Stale held-slot flags on settled activations do not affect compact status.
The private `harflow.workflow.worktree` v1 lookup receipt qualifies product/version, selected state
root path, run ID and canonical worktree/source paths; it is not part of the public fixture bundle.
Source descriptor identity is checked within each read; no device/inode value binds a receipt
across restarts. These receipts locate paths and never grant execution or removal authority.

### Retained-history completion and ownership

Optional workflow-run `finished_at` is nullable finite numeric Unix seconds, persisted
and projected in compact/history headers. It records the latest nonterminal-to-terminal
status transition, clears on reopening, and remains independent of settlement. Terminal
bookkeeping never invents missing historical completion evidence from `updated_at`.
Optional workflow-run `resumed_at` is finite numeric Unix seconds, persisted and projected in
compact/history headers and run detail. Absent means never resumed. The engine
stamps it with the same time as `updated_at` only when the caller continues the run: a first-accepted
`decision_id` answer, `resume_workflow` or `recover_workflow` leaving `needs_input`,
`needs_attention`, `paused` or `failed`, a newly reserved scheduler recovery, or a builder follow-up
that restarts the builder; a repeated recovery request while one is pending does not stamp again.
Node and decision activity, supervisor starts, engine retries, hook pauses, reconciliation,
parent-orchestrator answers, queued feedback to a running builder and receipt replays never change it.
An answer routed through a root run stamps the root, not the child that asked.
Private optional `finished_at_sequence` is a nonnegative exact integer witness matching
`sequence`; a numeric completion is qualified only with that witness and terminal status.
Older writers invalidate completion by advancing sequence alone. Reads expose unknown
without rewriting state; new bookkeeping cannot recertify stale dates, while a fresh
terminal transition records new evidence. Root and child creation initialize null with
witness zero. Task projection 3 and run projection 3 use `.listing.v11.sqlite3`, preserving
older derivative namespaces and their sidecars while retaining the shared lock.
Optional `workflow_id` derives only from the pinned run definition; modern IDs identify
definition generations and distinguish delete/recreate without matching the current
name. The existing legacy name-hash fallback
can be ambiguous between same-name legacy generations. Builders omit projected identity.

Task page `workflow_run_id: null` explicitly establishes that no ownership receipt was
present at observation time; a qualified string establishes workflow ownership. Missing
metadata means unknown, including on older engines. Malformed or unreadable receipts
make counts/history incomplete. This changes projections only, not TaskRecord storage.
Task completion remains nullable ISO8601, while workflow completion uses Unix seconds.
All fields remain additive wire v1 and run-document v1. The canonical task/workflow page
metadata fixtures are synthetic compatibility vectors covering explicit standalone and owned tasks, pinned workflow identity,
known completion and legacy missing evidence. See the [retained-history guide](compact-task-reads.md#retained-history-metadata)
for bounded headers, derivative rebuilds and summary completeness requirements.

## Finished retained-history statistics

`harflow-ctl statistics-page --since=<ISO8601> --before=<ISO8601> --limit=100 --json`
exposes capability `statistics-page.v1` through the existing `harflow.ctl` v1 result envelope.
Both boundaries require time zones; `since` is inclusive and `before` exclusive. Clients derive
local calendar-day boundaries, including daylight-saving changes, before sending UTC instants.
This global read requires a verified direct caller; managed callers and undecidable caller
identity are refused before history or derivative caches are read. It is not an MCP task tool.

Results report `projection_version`, `state` (`preparing`, `ready`, `unavailable`), `snapshot_id`,
`generation`, `observed_at`, flat `items`, `next_cursor`, `has_more`, `history_complete`,
`ownership_complete`, `accounting_coverage`, `retained_history_only`, `retention`, and bounded
`diagnostics`. Canonical examples are `contracts/v1/ctl-statistics-page-*.json`. Clients must finish
all pages of one generation before treating aggregates as authoritative. A cursor binds schema,
lexical state-root identity, bounds, stable order and generation; it is at most 4096 encoded bytes.
A relevant mutation returns `snapshot_expired`; restart pagination. Missing or incompatible
sources, ambiguous ownership, bounded-history exhaustion and unavailable ownership never produce
partial authoritative totals. Unsupported engines require a capability-gated unavailable screen.

Workflow records (`kind: workflow`) contribute the qualified finished root's `wall_seconds` on
its root start day. Root completion requires a matching `finished_at_sequence`; surviving children
never substitute for root timing. Terminal descendant executions, nested workflows, native logical
children and control transports join that same cohort regardless of their own dates. Finished
children of an active root are excluded. Standalone terminal initial/resume/continuation executions
enter by their own start time. Ancestor identity can cross the range boundary without adding its
usage or counts. Execution records contain nullable requested harness/model identity, task/chain/
workflow/node/activation IDs, start/end timestamps, duration qualification and accounting.
Requested model null means Unknown; effective or observed models never replace it. Filtered chart
and ranking time sums matching `execution_seconds`; unfiltered time uses root wall time plus
standalone execution time. These measures differ when work overlaps.

Accounting is an optional bounded v1 object written atomically with the task outcome. Legacy
records remain usage-unavailable; current writers distinguish pending normalization. Adapter
failures are absorbed and persist unavailable evidence without changing the terminal outcome.
Stable task contribution IDs replace cumulative snapshots rather than adding observations.
Categories (`input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_creation_tokens`) are
nullable integers; missing is not zero. `total_tokens` additionally requires qualified disjoint
categories and execution scope. Execution `duration_additive` qualifies time independently of token scope; `usage_additive`
requires independently owned execution-scoped usage. Execution counts are independent of both
flags. Native logical copies reference their transport contribution and set both flags false
unless separately proven disjoint. The nested `accounting.additive` flag qualifies token scope. See
[backend accounting qualification](backend-cli-facts.md#durable-execution-accounting-qualification).

The separate disposable `.statistics.v1.sqlite3` projection leaves listing v11 unchanged. Each
foreground call classifies at most 100 metadata documents using the existing metadata-byte budget;
there is no detached preparer. Discovery caps raw directory names at 100000 and candidate task/
run/ownership documents at 10000. A two-second foreground deadline covers enumeration, reads,
SQL, projection, source validation and page serialization. Pages contain at most 100 facts and
256 KiB. Calls return preparing while bounded work remains; hard limits return explicit incomplete
coverage. Cache/source projection bounds prevent uncontrolled cache growth. Unknown/corrupt
schemas rebuild only the derivative. All cache writes require owned compatible state.

Source witnesses include device, inode, nanosecond modification/change times and size, plus run
sequence in query generation. Directory witnesses are roster hints, never generation inputs.
Sources are checked around publication; active rewrites are ignored only after bounded
classification confirms they remain active with unchanged ownership. Required active task ancestry
binds its task/parent/continuation/chain/root identities and nonterminal classification in the
generation while its physical witness is still revalidated. Mutable active-ancestor display or
metric fields are not returned. Terminal transition or changed ancestry expires the page walk. Unrelated lock/artifact/
follow-up files do not change finished history generation. The observation timestamp reports a
validated observation, not a lock against future filesystem writes; changes without observable
filesystem witnesses cannot be detected.

Statistics describes retained history only. `retention` reports days (default 30), disabled or
unknown; selecting 90 days does not imply 90-day history exists. This query never runs retention,
reconstructs deleted data, replays raw logs, starts models or scans N task statuses.


## Recent usage-limit hits

`harflow-ctl limit-hits [--backend B] [--model M] [--since ISO8601] [--repo PATH] [--group G] --json`
and the MCP tool `recent_limit_hits` expose capability `limit-hits.v1` through the existing
`harflow.ctl` v1 result envelope. Require the capability on the fresh handshake; an older engine
answers the command with a usage error, which is "unsupported". The read is limited to a verified
direct caller: managed callers, other agent callers and undecidable caller identity are refused
before the catalog is opened (MCP `invalid_params`; ctl `agent_caller` or `caller_undecidable`). It
never retries, continues or switches a provider. Behavior and caveats are in
[usage limits](usage-limits.md).

`result` carries `hits`, `scan` and `index_state`. Each hit has `backend`, `requested_model`
(nullable), `task_id`, `title` (nullable), `group` (nullable), `observed_at` (UTC ISO8601),
`observed_at_source` (`finished_at` or `started_at`), `diagnostic` (`category` `usage_limit`, plus
the bounded `source`, `reason`, `provider_code` and `http_status` the record holds) and `reset`.
`reset.state` is `future`, `elapsed` or `unknown`; `raw` (the persisted epoch-seconds number or
offset-qualified ISO8601 string) and `at` (UTC ISO8601) are present for a usable value. A value that
fails validation omits both; a value that validates but cannot be represented as a date keeps `raw`
and omits `at`, with state `unknown`. `observed_at` is when the task settled, not necessarily when the
provider rejected the request.
`elapsed` is not availability. `scan` is `{examined, limit: 500, complete}`; `index_state` is the
listing catalog state (`status` `ready`, `preparing` or `blocked`). Canonical examples are
`contracts/v1/ctl-limit-hits-{ready,empty,preparing}.json`; `version-info.json` now lists the
capability, so HarflowMac must re-vendor the three fixtures, `version-info.json` and the manifest.

Hits come from the `task_limit_hits` side table of the disposable listing catalog, written from a
task record whose `failure_diagnostic.category` is `usage_limit`. Task projection 3 backfills it;
the catalog stays `.listing.v11.sqlite3` because the table is created in place and the projection
bump rebuilds the task headers. A query takes the newest 500 rows in `(observed_at, id)` order that
satisfy the indexed `since`, `backend` and `model` bounds, isolated in a subquery so equal timestamps
cannot widen it, then applies `repo` and `group` inside that window and keeps the latest hit per
`(backend, requested_model)`, at most 50 pairs. There is no pagination. An unreadable catalog answers
the ctl error code `limit_hits_error`.

## Workflow caller input projections

Workflow status, wait and resume payloads add optional `pending_inputs`, `pending_attention`,
`pending_input_count` and `pending_attention_count`. Input entries identify the source run/name,
node/title and verified invocation path, with `decision_id`, full `question`, `question_items`,
`waiting_since`, `waiting_seconds` and `optional_review_skip_available`. Attention entries carry
source identity, status, reason and waiting duration. Singular input fields remain compatible.
These additive fields are available to the macOS consumer through the existing public ctl
interface; rendering the list is a separate application feature. No canonical v1 fixture or
manifest digest changes are required for these optional additions.

`input_delivery_receipt` contains `workflow_run_id`, `decision_id`, `accepted_at` and `delivery_id`.
It confirms durable acceptance by that source, not completion of the resumed work. Persisted run
records add optional `input_deliveries` (exact per-decision payloads), `accepted_inputs` (acceptance
timestamps), `input_delivery_receipt` and `input_question_items`; accepted decision records may
include `question_items`. Parent child-question records may include a stable `checkpoint_id`
separate from the public question identity to prevent automatic answer loops. These fields retain v1 qualification and existing legacy delivery
receipts. Replies preserve complete actionable questions and item lists even above the compact
informational byte target; informational output remains bounded. New input presentations after
acceptance use fresh decision identities.

## Backend qualification receipts (private state)

`<state_root>/qualification/<key>.json` stores private feature qualification evidence. It is not a
public wire contract or proof of binary authorship. Schema 1 carries `product: "harflow"`,
`format: "harflow.qualification"`, `product_version`, integer `v: 1`, an `identity` object,
`revision`, and `features`. Product and storage-line compatibility are checked before reading any
feature. Missing, incompatible, malformed, oversized or unreadable evidence cannot authorize a
feature; no automatic repair or conversion is provided.

The identity contains backend name, observed version, resolved executable path, executable
SHA-256 and platform. The key hashes that identity together with the backend's probe revision.
Each feature row contains boolean `ok`, `source` (`seeded`, `auto-probed`, `failed`, or
`failed-at-runtime`), bounded `reason`, and a facts object. A successful row must establish its
named feature. Seeds trust the maintainer's measured version/platform, rather than proving the
installed executable's provenance. Runtime failures target the original launch identity and
invalidate the dependent bundle, including over a seed. Explicit refresh is required to retry a
failed result.

The complete receipt and probe response are bounded to 64 KiB. Cache reads and writes use the
selected store's ownership guard. A per-identity/revision file lock serializes the probe bundle;
receipts publish by atomic replacement after flushing. Directory-relative descriptors and
no-follow opens reject symlink cache directories, locks and receipt files. Inspection opens only
existing ownership infrastructure, never initializes or promotes the store, and never runs a
provider probe. Private cache files remain subject to the existing cooperative same-user state
boundary; they are not authenticated against a malicious same-user writer.


### Task outputs and inherited child specifications

Permission previews add optional `task_outputs` guidance with deterministic report and
plan paths, the engine writer identity, and `agent_writable: false`. Final drained task
summaries are published under `<state root>/outputs/<task_id>/report-<task_id>.md`;
workflow JSON `result.technical_plan` is also published as `plan-<task_id>.md`.
Existing artifact references expose these paths with additive private provenance
`method: engine_output`. Publication grants no agent filesystem permission and does not
certify content validity. Output files are retained and pruned with their task.
Existing readers accept the new registrar string and optional fields; canonical fixtures
and independent Mac wire-version gates are unchanged.

Private workflow invocation and child-run records may carry `inherited_context`, a
versioned SHA-256-bound snapshot of the parent request, selected predecessor inputs and
technical-plan revision at invocation preparation. The snapshot is reused after restart,
validated on read, and delivered independently of child graph traversal. Large snapshots
use complete digest-bound private temporary files rather than truncated prose. A short-line
JSONL string-chunk companion supports ordinary paged Read tools in plan mode, with complete
reconstruction bound to the original file digest. These
fields are private storage additions; no public event kind or wire requirement changes.

### Caller loss and workflow interaction handover

Workflow status/detail projections add `interaction_state`: `owner` (`caller` or
`monitor`), `caller_state` (`alive`, `dead`, `unknown`), `monitor_available`, a
human-readable `reason`, and nullable `handover`. A handover records `timestamp`,
`reason`, `previous_owner`, `responder`, and `action`. These fields contain no
process command lines. Full questions remain in `input_question` and
`pending_inputs`; interaction metadata does not replace their existing fields.
Availability is advisory; mutation rechecks authority and the current checkpoint.

A verified human `--monitor` responder may take a pending interaction only after
qualified caller identity confirms process absence or PID reuse. Unknown identity,
failed probes and missing historical evidence refuse handover. The accepted action
and root authority/audit are persisted under the tree mutation lock. Descendants
inherit root authority, and returning callers cannot answer after handover. Existing
scheduler recovery, permission, decision and goal revision gates still apply.

Admission captures the managed dispatching task's qualified process identity, or
an external executable client ancestor before detaching. The stdio server and ctl
process themselves never establish caller loss. Known transparent `uv`, `uvx`,
`env`, `nohup`, and `setsid` launchers are traversed with bounded, revalidated
parent links. Shell/script/interpreter launchers and undecidable ancestry remain
unknown; a client should launch Harflow directly or through a supported transparent
launcher. Server death alone does not establish client death. Existing runs without
admission evidence remain caller-owned. No live-caller opt-in is implemented.

`ctl-workflow-interaction-handover.json` is a synthetic public-response fixture
showing a handed-over root and its audit; it does not certify a real caller loss.

## Backend qualification discovery

Capability `backend-qualification.v1` qualifies cached-only launch requirement discovery:

```sh
harflow-ctl permission-preview --repo PATH --backend BACKEND --qualification-only --json
harflow-ctl workflow-preview NAME --repo PATH --qualification-only --json
harflow-ctl qualify BACKEND --feature caller_approval_tools --feature caller_write_tools --json
```

Existing permission and orchestrator override flags apply to discovery. It returns the
ordinary ctl v1 `result` envelope containing `qualification_version: 1`, `discovery_only: true`
and `requirements`. Each requirement contains a stable `id`, `backend`, `required_features`
and a `features` map keyed by required feature. Standalone origin is `task`; resolved workflow
origins are `<workflow_id>/orchestrator/<position>`, `<workflow_id>/node/<node_id>/<position>`
and `<workflow_id>/on-demand/<node_id>/<position>`, including primary position zero and all
fallbacks. Orchestrator origins identify only actual owners: the root and Child-mode targets,
including Child boundaries reached through Current-mode descendants. An unused orchestrator
of a Current-only descendant is omitted; that descendant's nodes remain included. A workflow
referenced in both modes contributes its owner once. Multiple references to the same pinned
workflow share its stable workflow identity. Discovery rejects invalid or unresolvable definitions;
it never returns a truncated authoritative plan. Enumeration stops at 4096 origins. Incremental
ASCII-escaped JSON byte accounting rejects a row before it exceeds the 1 MiB ctl envelope
budget, including the trailing newline; repeated cached facts cannot build an oversized document.

The required set is derived by the same pure policy used by launch permission validation:
`caller_approvals` requires `caller_approval_tools`; `caller_write_approvals` additionally
requires `caller_write_tools`; nonempty `allowed_commands` requires `allowed_commands`.
No requested gated features means an empty required set and empty feature map. Such an entry
satisfies qualification without inspecting the binary and omits identity. This does not establish
binary installation or launch eligibility: normal launch validation remains authoritative.
Extra writable paths retain their independent existing certification gate and are never
represented as these three permission features.

For nonempty required sets, optional `identity` identifies backend, observed version, executable,
SHA-256 and platform. Every feature evidence row binds that same identity and includes boolean
`ok`, `source`, bounded `reason` and available revision/facts. Undeclared or missing required
features are unavailable, never implicitly successful. A bounded `unavailable_reason` identifies
missing identity/evidence; unavailable identity is omitted rather than fabricated. Consumers must
require every requested feature's `ok` and matching identity; version display alone authorizes
nothing. Discovery may invoke `--version`, hash executables and read existing qualification
receipts or maintainer seeds. It never invokes provider self-tests or starts an agent. Evidence
is deduplicated per backend while every origin remains visible. Launch revalidates current inputs.

`qualify --feature` is repeatable; omission refreshes all declared features. Repeated feature
names are deduplicated and each shared probe revision refreshes once. Existing single-feature
invocations preserve their result shape and exit semantics. Failed feature results always include
a source and a reason bounded to 1000 characters; unavailable inspection uses source `unavailable`
and omits identity rather than fabricating one. Explicit qualify is the action that
may run isolated no-paid provider self-tests; discovery never starts them.


### Compact MCP polling and workflow discovery

MCP `get_task_status` and `wait_for_task` default to `detail="compact"`.
`detail="full"` retains the previous complete MCP metadata; `include_tail=true` explicitly
includes the raw tail in either projection. The compatibility `brief=true` parameter selects
compact status and now preserves the complete summary. Compact replies retain the full
`summary` (including a final report), status, approval and failure evidence, outcome headline,
recent activity and detail references. They omit static enforcement and MCP-server inventories,
raw outcome probes, `dispatch_origin` and diagnostic `supervision.dispatch_origin_state`.
Their overhead is independent of those omitted inventories; complete reports and outstanding
interaction evidence intentionally have no fixed display-size cap. `get_task_events` retains
its existing event/page limits; read the complete final report from compact task status.
No persisted task snapshot or ctl projection is replaced by this MCP projection.

All waits recommend explicit `timeout_seconds <= 50` for remote MCP clients. The single-task
default is 50 seconds. Requested supported holds remain unclamped; a client deadline can still
end a call before the server finishes observing, without cancelling execution.

MCP `list_workflows` defaults to compact entries with `name`, `description`, `revision` and
`workflow_id`, without node instructions or graph bodies. `detail="full"` returns complete
definitions and `get_workflow(name)` is unchanged. Source compatibility/availability validation
still applies to the entire listing. `harflow-ctl workflow list`, including the Mac's monitor
projection, is unchanged; it does not call the MCP listing tool.

### Surviving execution after dispatcher restart

Optional additive `supervision.adopted_after_restart` contains `{at, previous_server,
current_server}` when the dispatch origin is confirmed dead, the execution owner is alive,
the agent is not confirmed dead, capture is live, and live input is neither lost nor unknown.
This describes adoption of observation only: execution ownership and task state never change.
`at` is the current reader's process start time converted from local `ps lstart` evidence to
UTC, or null if that timestamp cannot be parsed. It is not a measured instant of the old
server's crash or an ownership-transfer timestamp. The previous/current identities contain
only PID and process-generation start time. Full MCP reads retain this stable metadata on every
observation; compact MCP replies emit it and its quiet observation note once per task and reader
generation. Display deduplication is in-memory, shared by clients of that MCP process and
bounded to 1024 recently observed tasks; eviction or a new process can re-emit the note.
There is no acknowledgement ledger and waits write no execution state.

A dead origin with lost or uncertain supervision remains highlighted as `supervision_issue`
in running compact replies, alongside the actual owner/process/capture/input evidence.
Full diagnostic reads retain dispatch identities and their liveness verdict. This is an
additive optional v1 field: existing fixtures and consumer gates remain unchanged. The Mac
presentation of this optional metadata is deferred; it needs a separate design and consumer
implementation before claiming the app renders a restart note.

Task enforcement may additionally contain `git_environment`, with `github_https_rewrite` (boolean),
`https_auth_observation` (sanitized string), and `reachability_verified` (false). While the rewrite
is on, `gh_missing`, `gh_not_authenticated` and `gh_authenticated_helper_not_observed` also carry
additive `remedy` (string): the HTTPS setup instruction, including a server restart. Other
observations and a disabled rewrite omit `remedy`. This optional
observation is not authority and does not change network enforcement. Old records omit it; resumed
and continued tasks inherit a saved effective rewrite when present. `list_backends` also exposes
this optional observation using the server default. See [Git transport](git-transport.md).

Dispatch-time command grants may additionally contain `extra_allowed_command_matches`, a list of
`{pattern, match}` objects next to `extra_allowed_commands`. `match` is `exact` or `prefix`. The
field is omitted when the task has no grants. Existing `extra_allowed_commands` strings are unchanged.
See [harness permissions](harness-permissions.md#dispatch-time-command-grants-allowed_commands).

### Scoped group report output

`group_report_output` is an optional boolean launch/candidate setting. It requests a qualified
headless read-only repository policy with a write exception for the task's resolved group scratch
directory. It does not authorize repository writes, task or chain scratch writes, caller-approved
escapes, native children or interactive takeover. Unsupported backend/version/platform/configuration
combinations refuse the request explicitly. Omission preserves historical behavior; headless resumes
and continuations inherit the saved effective setting, while an explicit false disables it.

Optional `enforcement.group_report_output: true` records the effective exception. The existing
`writable_roots` and `scratch.access.group` disclose its exact scope. Permission previews accept a
group and disclose `group_report_directory` without allocating it, checking the same configuration
requirements as launch. Saved workflow candidates requesting the feature use the root run's stable
`workflow:<root run id>` group, subject to the existing nested caller authority checks.

Optional `enforcement.local_commit` is `denied`, `allowed`, or `not_enforced`. It reports whether a
direct local `git commit` is denied by the harness policy, allowed, or not something Harflow can
claim. Claude Code is `denied` at `read_only` and `write_in_repo` and `allowed` at `publish` and
`unrestricted`. Every other backend is `not_enforced` at every freedom. A merge-conclusion grant
does not change it. Old records omit the field; a missing parent field is not compared.

Optional `enforcement.merge_conclusion` is present only when the task was granted exact
`git commit --no-edit` or `git merge --continue`. It records `requires: MERGE_HEAD`,
`ordinary_commits: refused`, whether the broad commit deny was omitted, and `gate: path_wrapper`.
When the commit deny is omitted, `direct_commit_commands_denied` is false.

Optional `enforcement.dispatch_git` and top-level task `dispatch_git` record branch preparation and
fetch Harflow ran before the agent. They name the branch, base, base commit, each fetch, when it
ran, and any notice. A preview uses `status: preview` and does not run git. Old records omit both.

These three fields are additive. The canonical `contracts/v1` fixtures and `manifest.sha256` are
unchanged, so existing consumer gates stay valid. Readers must tolerate their absence.

The group is shared authority, not per-file ownership. Concurrent tasks should use task-identifiable
report filenames and atomic updates; peers can overwrite a file with the same name. Scope identity
and qualified configuration are checked before launch, without an atomic pin against independent
external writers. See [shared scratch](shared-scratch.md) for the supported backend matrix and limits.
These are optional v1 additions; canonical fixtures and existing consumer gates are unchanged.


### ctl parent observation continuity

`status` and `status-batch` accept optional `--observer-parent`. This read-only mode
qualifies the actual immediate parent using PID plus process-generation start time,
once per invocation (shared by every batch entry). No caller-supplied identity is
accepted. The parent must be confirmed alive; absent, dead, changed or undecidable
identity produces no adoption and never falls back to the ephemeral ctl process.
Task fields add `observer_kind: "parent_process"`, nullable `observing_process`
(`{pid, start_time}`), and tri-state `observer_state`. `responding_server` remains
the actual ctl responder; observation mode describes the qualified parent.

For this mode, `supervision.adopted_after_restart` has `{at, previous_server,
observer_kind: "parent_process", observing_process}` and omits `current_server`.
The same dispatcher/owner/process/capture/input gates apply. `at` is the parent's
local process-start evidence converted to UTC, not the polling time. Default ctl
status and list reads preserve identity and liveness diagnostics but suppress synthetic
adoption by their ephemeral process. Actual MCP server adoption retains its
existing `{at, previous_server, current_server}` shape. Nothing writes ownership
or task state; older consumers can ignore these optional v1 fields.


### Id prefixes

Every MCP tool and `harflow-ctl` command that takes a task id or a workflow run id also accepts a
unique prefix of at least 8 characters (`id_resolution.MIN_PREFIX_LENGTH`). The id is resolved at
the boundary and the full id is what the operation uses; a value that is already a full id is an
exact hit if it is resolved again further in. The one command that never takes a prefix is `worktree-remove`,
which requires the full id because it is destructive and accepts a task or a run id in one namespace.
`start_workflow` does not resolve anything: it creates a run.

Resolution order:

1. Authority first. A managed workflow caller resolves only within the ids its existing guards allow
   for that operation (task reads: its own execution or run; run reads: its own run; linked
   child runs: the persisted parent chain; assigned inputs: the `authorized_input_refs` of its own
   activation for that execution). A value outside that namespace is passed on unchanged, so the
   existing refusal answers identically for a missing and for an unauthorized id, and an
   unauthorized id is never counted as a candidate. Direct callers resolve against the current state
   root's catalog.
2. An exact id always wins and is returned unchanged. A value in canonical UUID form (36-character
   hyphenated or 32 hex digits) is never a prefix: if it does not exist it is unknown.
3. A value shorter than 8 characters that is not an exact id is `unknown_task` /
   `unknown_workflow_run` with `data.reason: "prefix_too_short"` and `data.min_prefix_length: 8`.
4. Otherwise the listing catalog's completed discovery generation is searched with a primary-key
   range query. Discovery is retried once; if it still cannot complete, the result is `unknown_*`
   with `data.reason: "prefix_index_unavailable"` and no id is guessed. Pending launches that have no
   record yet match by exact id only. The record file is checked again after a match; one that
   vanished is `unknown_*`. No match is `unknown_*` with `data.reason: "no_prefix_match"`.
5. More than one match is `ambiguous_task_id` / `ambiguous_workflow_run_id` with `data.candidates`
   (at most 5 of `{task_id|workflow_run_id, title|name, status, created_at}`, from catalog headers;
   fields are `null` while a header is not built) and `data.truncated`.

Surfaces that already answer a missing exact id in their own way keep doing so: approval list, get
and respond, `get_task_artifacts` / `task-artifacts`, the notification reads and `unsubscribe`
calls, and the explicit-id lists below pass a missing exact or canonical id through unchanged
(an empty approval page, `approval_error` with reason `not_current`, `artifact_error`, an empty
subscription list). They report only `ambiguous_*` and real prefix failures (a non-canonical value of
at least 8 characters with no match, or an index that is unavailable). Catalog read failures are
reported as `prefix_index_unavailable`; ownership and compatibility errors still propagate.
On the approval and artifact surfaces only a canonical (UUID) id or a value shorter than 8 characters keeps the passthrough; a non-canonical value of at least 8 characters that matches nothing, such as a custom id, is `unknown_*` with `reason: "no_prefix_match"`.
`inspect_workflow_node` resolves within the caller's own run; only run-level artifact reads
(`get_workflow_artifacts`) and linked-child inspection use the verified tree of a managed
orchestrator, and a tree larger than the bounded listing is never resolved against.

Matching is case-sensitive and uses the id's own character set (task ids `[A-Za-z0-9_-]`, run ids
also `.`); any other character keeps the existing invalid-id error. Run-local task ids
(`task_id` on workflow messages, node inspection and run artifacts) resolve against that run's
`activations[].tasks[].task_id`; `read_workflow_assigned_input` resolves `workflow_run_id` against the
caller's own run first, then `source_run_id` and `task_id` jointly against the caller activation's
assigned inputs, never mixing references from other activations or executions.

Errors: MCP errors carry `data.code` (`unknown_task`, `ambiguous_task_id`, `unknown_workflow_run`,
`ambiguous_workflow_run_id`) plus `data.input`; the ctl `error` object uses the same `code` and puts
the same fields in `error.data`. `harflow-ctl resume` and `continue` keep reporting `invalid_params`
for an unknown id (the specific code is in `error.data.code`). **Code change:** a workflow ctl
command naming a run that does not exist now answers `unknown_workflow_run` where it used to answer
the generic `workflow_error`.

Replies: when the input was a prefix, the reply gains `resolved_from: {input, task_id}` or
`{input, workflow_run_id}` (the resolved id; a continuation's own `task_id` keeps its meaning).
Further id arguments add `resolved_from_<argument>` (`resolved_from_task_id`,
`resolved_from_source_run_id`, `resolved_from_related_run_id`, `resolved_from_predecessor_task_id`),
and list arguments add arrays (`resolved_from_task_ids`, `resolved_from_run_ids`) holding only the
entries that were prefixes. A reply that is a bare list (`get_task_notifications`, `unsubscribe`)
carries `resolved_from` on every row. `status` puts it in the `task` object. `status-batch` keeps one
row in `tasks` per input (identical raw inputs are still collapsed; two different prefixes naming one
task give two rows), each row with `resolved_from` when its input was a prefix, and `errors` stays
keyed by the raw input. Pages that list explicit ids (`task-list-page --task-ids`, `task_ids`,
`run_ids`) still drop a missing canonical id silently; a missing non-canonical value is reported.
`wait_for_tasks` decides uniqueness on the resolved ids, so two prefixes naming one task are refused
as duplicates.

The new fields, `data` members and error codes are additive; unknown optional fields are ignored, and
the `ctl-batch.json` fixture shows a prefix row. The exception is the documented ctl code change:
`unknown_workflow_run` replaces `workflow_error` for a missing run.

### Bounded workflow run filters and field projections

`list_workflow_run_page` (MCP) and `harflow-ctl workflow-list-page` accept optional filters; every
one is additive and an older engine rejects an unknown ctl flag with a usage error. Runs have no
group, so there is no `group` filter.

| Filter | ctl flag | Meaning |
| --- | --- | --- |
| `workflow_id` | `--workflow-id` | Stable workflow identity of the run (the `workflow_id` header field; legacy runs use their derived `legacy-` identity). |
| `repo` | `--repo` | Exact `repo_path` recorded for the run. |
| `status` | `--status a,b` | List of run statuses: `pending`, `starting`, `building`, `running`, `pausing`, `paused`, `needs_input`, `needs_attention`, `cancelling`, `completed`, `failed`, `cancelled`. Matches the indexed status shown on each item, which is the stored status, not a status derived from child runs. |
| `since`, `before` | `--since`, `--before` | `created_at` bounds as ISO8601 timestamps with a time zone: `since` inclusive, `before` exclusive. |
| `title_contains` | `--title-contains` | Case-folded substring of the run name. |
| `scope` | `--scope` | `all` (default, the unfiltered behaviour), `root` (runs without a parent) or `nested` (child runs). |

Filters combine with AND, are validated before any read (unknown names and statuses report
did-you-mean hints), and cannot be combined with `run_ids` or `related_run_id`. MCP refuses these
with invalid params; `harflow-ctl` reports `workflow_error`. `active_only` still applies. Filter values come from a lossless `run_meta`
side table of the listing catalog, never from a truncated header.

Filtered pages are bounded by a scan window. A page examines at most 2000 catalog rows after its
cursor, in `(created_at DESC, id DESC)` order, and applies the filters inside that window. When the
window ends before the end of history, the reply has `has_more: true` and a `next_cursor` positioned
at the last examined row, so a sparse filter makes progress on every call; keep calling until
`has_more` is false. `scan_incomplete: true` additionally means the page did not fill (it carries
the matches found so far); a page that fills, wherever its last match sits, has
`scan_incomplete: false` and still carries `has_more` and the cursor. A page length, including zero, never means the end of history.
Filtered replies carry `scan_incomplete` (absent on unfiltered replies) and report
`total_active_count: null`: filtered totals are not computed. `total_active_root_count`,
`total_attention_root_count` and `counts_complete` keep their unfiltered meaning. A cursor is bound
to the filters that produced it; one reused with different filters, or between a filtered and an
unfiltered listing, is refused as an invalid cursor. A catalog that is still indexing reports
`bootstrap_pending` and its usual state fields exactly as an unfiltered page does.

Runs whose header could not be built (no `run_meta`, status `unknown`) can match only `since` and
`before`, which use the indexed creation stamp; `history_incomplete` signals that such runs exist.

A workflow-managed builder or orchestrator that lists runs sees its own run only; the filters are
evaluated against that run's own metadata (including `repo`), so a match returns a page of one and a
mismatch an empty page. Worker nodes remain refused.

`fields` (list of field names; ctl `--fields a,b`) selects keys after the reply is fully built, so
values equal the unprojected reply and there is no header-only shortcut. `workflow_run_id`,
`status`, `created_at` and `updated_at` are always included, a selected field keeps its
`<field>_truncated` marker, placeholder diagnostics (`needs_direct_lookup`, `index_blocker`, `note`)
are never dropped, and while a run is `needs_input` its question fields (`input_question`,
`input_question_items`, `question_items`, `pending_inputs`, `pending_attention`) and the fields
needed to answer (`input_decision_id`, `interaction_owner`, `pending_input_count`) stay. A page with
`related_headers` keeps each item's `parent_workflow_run_id` and `orchestrator_session_owner_run_id`. Unknown names
are refused with the accepted list and a did-you-mean hint. On `list_workflow_run_page` it projects
`items` and `related_headers` from the listing header vocabulary; on `get_workflow_status` it
projects the compact status, validated against the compact status fields (a managed caller's summary is projected the same way); on
`harflow-ctl workflow-status` it projects the reply's top-level keys (not with `--monitor-view`).
Omitting `fields` leaves every reply unchanged.

The listing catalog is now `.listing.v11.sqlite3` (schema version 11, adding `run_meta` and, with
task projection 3, `task_limit_hits`). Run projection 3 is unchanged; older engines keep their own `.listing.v10`
namespace and sidecars, and the new catalog rebuilds through bounded bootstrap. No `contracts/v1`
fixture changes.

## Provider failure diagnostics

Task status and persisted task records carry additive `failure_diagnostic` metadata with
`category`, a bounded safe `reason`, and `source`. The optional `http_status` integer and
`provider_code` string retain established provider evidence; URLs, headers and arbitrary
provider response text are not copied into this metadata. `provider_code` is present only
for a recognized backend-owned code, never inferred from prose. Wait summaries keep the
bounded category and reason; terminal notification payloads and workflow execution/result
diagnostics retain the safe cause. Existing consumers may ignore the optional fields.

For Vibe 3.0.1, the measured HTTPX stderr pair for HTTP 402 followed immediately by
`Usage limit exceeded` establishes `category: usage_limit`. A typed Vibe error with an
explicit 402 and recognized `usage_limit`, `usage_limit_reached` or `insufficient_quota`
code does too. Other 402 responses use `category: provider_error`, with a safe billing/usage
explanation. HTTP status alone never establishes a usage limit. No reset time is invented
and classification never retries, continues or switches providers automatically. See
[the qualification evidence](backend-cli-facts.md#vibe-http-402-diagnostics).

### Monitor workflow control eligibility

`workflow-status --monitor-view` includes additive `monitor_controls`. Each key
(`pause`, `resume`, `recover`, `revise_goal`, `skip_optional_review`,
`additional_attempts`, `cancel`) contains `allowed`, `reason`, and
`target_workflow_run_id`. Disabled actions remain visible with their reason.
Children identify the root route and require opening that root's current
checkpoint; a child's displayed answer must not implicitly answer a root question.
Missing fields on older engines mean eligibility is unknown, not granted.

These fields are advisory observations, not authority or durable grants. Every
command rechecks current ownership, checkpoint, execution and takeover state.
The Monitor always identifies itself with `--monitor`; it cannot impersonate a
live caller. Only qualified caller death permits the existing atomic interaction
handover for resume, recovery and goal revision, and for explicit queued message
send/cancellation. Pause does not claim caller authority. Pending questions,
optional-review consent and additional attempts retain their existing decision
and reason requirements. Rendering these controls never claims ownership.


## Queued task and workflow messaging

The fresh handshake capability `messaging.v1` qualifies `send`, `cancel-message`, `workflow-send`
and `workflow-cancel-message`, plus the additive `messages` lifecycle on task and workflow status.
Consumers must require this capability before using the new modes. Public MCP equivalents are
`send_message`, `cancel_queued_message`, `send_workflow_message` and `cancel_workflow_message`.
The task forms route workflow-owned targets through their owning workflow, including builders.
Workflow forms accept a run ID and an optional `task_id` (CLI `--task-id`). All forms preserve
exact caller prose. `--monitor` / `interaction_owner: "monitor"` requires a verified human caller;
workflow interaction ownership and proven-dead caller handover match the other workflow controls.
Managed node callers cannot use messaging to change other assignments.

`delivery` is `queue` by default or `now`. Queue never folds into the current live turn. Standalone
messages wait for the next explicitly requested continuation; automatic continuation never consumes
them. Workflow messages wait for the next issued orchestrator or builder dispatch; a task target
waits for its node's next issued attempt, including when the original task settles first. Messaging
never issues a node, changes a goal, grants another attempt or widens permissions. Completed or
cancelled non-builder workflows mark waiting messages `undeliverable` with a reason; failed runs
retain queued messages for explicit recovery. Completed builders can take another builder turn.

Now on a conclusively settled standalone task requests the existing guarded fresh continuation.
On safely paused or attention-stopped workflows it uses the existing resume control with no answer,
retry grant or changed goal; the engine revalidates the current checkpoint. A completed builder
uses its existing builder continuation route. Pending questions, approvals, uncertain ownership,
attempt budgets and unsupported interruption remain barriers. No current backend adapter certifies
a safe mid-turn interrupt primitive. A now refusal is an accepted queued response carrying
`refusal: {code, message}` and the message ID; it does not roll back admission. Codes include
`interrupt_unsupported`, `workflow_checkpoint_required` and `continuation_refused`. Successful
continuation adds `successor_task_id` or `delivery_started`; it does not claim the model has read it.

Responses contain `message_id`, `seq`, `status`, `delivery`, and task/run IDs. A queued send to a
standalone task whose `resolve_status` is already terminal (`completed`, `failed`, `timed_out` or
`cancelled`) adds `target_settled: true`, `target_status` and a note that the message waits for
`resume_task` / `continue_task` or `delivery="now"`. A running target, including a recovered task
that is still alive, omits those fields. The same fields are added for a workflow or node message
when the owning run is cancelled, or completed and not a builder, because no further dispatch will
consume it. Failed runs and completed builders omit them. `delivery="now"` is unchanged.
Workflow status
`messages` entries contain `id`, monotonically increasing `seq`, `target` (`kind`, run ID and,
for node targets, task/node/role), `delivery`, `status`, timestamps, and optional text, reason and
`delivery_task_id`. Statuses are `queued`, `delivering`, `delivered`, `undeliverable`, `cancelled`.
Dispatch reservation atomically claims eligible messages in order; positive no-spawn evidence
releases a claim, while uncertain delivery never requeues it. Delivered means handed to an issued
process, not successful execution. Delivered/cancelled records retain metadata only. Task status
also keeps the existing `queued_followups` ledger (`reserved`/`consumed` map to
`delivering`/`delivered` in `messages`; uncertain consumption remains delivering with a reason).
Cancel accepts only queued records; claiming and cancellation serialize through the same lock.

Messages are limited to 32 KiB UTF-8 each and 16 outstanding per task/run. Workflow ledgers retain
at most 256 KiB of text and bounded terminal history. Compact workflow status returns text previews
and `messages_truncated`; `workflow-detail --view messages` pages the full retained messages.
The queue limits are admission limits, not additional execution or filesystem authority.

`workflow-build --monitor` (qualified by `messaging.v1`) records human Monitor ownership in the initial builder document before launch. The public MCP builder keeps caller ownership by default. Builder follow-up, including the legacy builder-followup tool, rechecks the current owner inside its feedback mutation; Send now additionally requires an idle builder there. A terminal workflow Send now response is `undeliverable` with `workflow_terminal`, never a promise that the message remains queued.
