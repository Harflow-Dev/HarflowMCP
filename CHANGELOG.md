# Changelog

## Unreleased

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
