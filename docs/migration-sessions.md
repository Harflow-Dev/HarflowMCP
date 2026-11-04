# HarflowMCP migration sessions

Status: S01–S06 evidence delivered 2026-11-04; S05/S06 receipt acceptance pending; S07 onward remains unstarted. The [workspace session index](../../HARFLOW_MIGRATION_SESSIONS.md) defines dispatch order; the [repository checklist](migration-plan.md) retains full item criteria. These session specifications include work led or co-owned by this repository.

## Dispatch protocol

Read the workspace `AGENTS.md` and `HARFLOW_MIGRATION_CHECKLIST.md`, the lead and co-owned repositories' `AGENTS.md` files where present, their `docs/migration-plan.md` files, applicable package guidance, and the session and all accepted prerequisite receipts before work. The workspace session index gives the full sequence. A session is a bounded assignment, not an instruction to start the next session. A standalone session lead owns the normal required plan/code reviews, test delegation and validation; a worker explicitly handed off by an orchestrator follows that handoff and does not restart its workflow. Stop and report a concrete unresolved ambiguity or missing prerequisite instead of inventing a decision.

Use primary checkouts and preserve concurrent work. Never share a writable state root between engines or import historical task/run/event state. Work on isolated roots and fixtures unless separately authorized. MCP owns canonical contracts until explicit S50 transfer. Cross-repository package changes require a committed producer, exact consumer pin and clean install without sibling source dependencies; a necessary push requires current authorization. Do not run concurrent sessions that mutate the same repository, generated contract bundle or IPC boundary. Within a session, coordinate delegated workers through explicit file ownership. Approval for publication, signing, paid probes, live state, registrations, cutover and retirement is separate and current; prepare concrete evidence first.

A session receipt is proposed at the lead repository's `docs/migration-receipts/<session>.md`. It records prerequisites, accepted scope and decisions, exact commits for every involved repository, contract/design/package/runtime identities, actual commands and results, platform and profiles, acceptance evidence, deviations, missing checks and authorization or blockers. Distinguish local, committed, pushed, CI-passed, packaged and released states. It contains technical evidence, not review transcripts. A multi-repository session has one lead receipt covering each repository. A receipt is accepted only after the applicable review and owner decisions. Delivered work and receipt acceptance are distinct; see session statuses below and the workspace reconciliation.

Checklist items may span sessions. Each mention is partial until the final contributing session identified below; even then, check the source item only after all contributing receipts and the full original item acceptance have been reviewed and accepted. The sequential order is the safe default; S25's isolated research exception and optional sessions are explicit. There is no duration guarantee. Defects or follow-up work discovered during a session need separately scoped assignments; they do not automatically expand or advance the session.

Copyable dispatch (replace bracketed inputs):

```text
Run session Sxx from HARFLOW_MIGRATION_SESSIONS.md and the lead repository's docs/migration-sessions.md.
Accepted prerequisite receipt paths and IDs: [paths and IDs]
Pinned contract, design, package and runtime identities: [commits, digests and paths]
Allowed isolated roots, fixtures and external actions: [scope and authorizations]
Return the session receipt at the specified lead-repository path, including all repository commits, evidence, decisions and blockers. Complete only this session; do not start the next.
```

For optional work replace `Sxx` with `X01`, `X02` or `X03`.

The [v2 coverage ledger](../../HarflowDesign/v2/docs/migration-coverage.md) is an optional workspace coordination reference, not a build dependency. S10-Gxx bounded gap sub-sessions follow scope approval and obtain exact design approval before dependent work; unresolved required states block only their dependent feature. Independent work continues. The approved mock does not establish runtime capability or authority. Existing S01 evidence and S02 status remain as recorded in their receipts.

## Sessions

### S01 — Contract and runtime inventory

- Status: Inventory delivered; receipt acceptance pending. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: None; record initial inputs.
- Checklist coverage: B0-MC-01 (final contribution), B0-MC-03 (partial).
- Scope and acceptance: Record commits, selected runtime, operation/schema inventory, fixture digest, capability/version cells, baseline defects and reproducible checks.
- Output: lead receipt `docs/migration-receipts/S01.md`, with the evidence and identities required by the dispatch protocol.

### S02 — Native pair qualification and references

- Status: Complete with recorded qualification limits (HarflowMac receipt). Lead: HarflowMac. Co-owned: HarflowMCP.
- Prerequisite receipts: S01.
- Checklist coverage: B0-MA-01 (final contribution), B0-MA-02 (final contribution), B0-MA-03 (final contribution), B0-MC-03 (final contribution).
- Scope and acceptance: Qualify the current selected Python/Mac pair, preserving dirty work; capture representative isolated states and document package versus source evidence and limitations.
- Output: lead receipt `docs/migration-receipts/S02.md`, with the evidence and identities required by the dispatch protocol.

### S03 — Freeze, parity and scope decisions

- Status: Decision record delivered; receipt acceptance and Proposed/Undecided scope outstanding. Lead: HarflowMCP. Co-owned: HarflowMac, HarflowDesign, HarflowEngine, HarflowWeb.
- Prerequisite receipts: S02.
- Checklist coverage: B0-MC-02 (final contribution), B0-MA-04 (final contribution), B0-DS-01 (final contribution), C0-DS-04 (partial), B0-EN-01 (final contribution), B0-WE-01 (partial).
- Scope and acceptance: Record Keep, Simplify, Deliberately remove and Undecided decisions in the v2 coverage ledger; obtain decisions on freeze, representative datasets and performance budgets, preserving unresolved scope and approved removals.
- Output: lead receipt `docs/migration-receipts/S03.md`, with the evidence and identities required by the dispatch protocol.

### S04 — Bounded Python public reads

- Status: Implemented with recorded verification limits, including S04-gaps.md; receipt acceptance pending. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S03.
- Checklist coverage: C0-MC-01 (final contribution).
- Scope and acceptance: Audit existing public reads against approved v2 and scope decisions, including task title, changes and diff provenance; deliver only needed approved additive bounded reads with expiry, gaps, drain and change evidence.
- Output: lead receipt `docs/migration-receipts/S04.md`, with the evidence and identities required by the dispatch protocol.

### S05 — Handshake and contract fixtures

- Status: Evidence delivered; receipt acceptance pending. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S04.
- Checklist coverage: C0-MC-02 (final contribution), C0-MC-03 (final contribution).
- Scope and acceptance: Reconcile v2 mock capabilities with actual public compatibility, errors and schemas; fixture handshake, unknown fields, run/resume and included MCP schemas, documenting unsupported capabilities without inventing semantics.
- Output: lead receipt `docs/migration-receipts/S05.md`, with the evidence and identities required by the dispatch protocol.

### S06 — Mac fixture compatibility

- Status: Evidence delivered with recorded validation limits and owner freeze waiver; receipt acceptance pending. Lead: HarflowMac. Co-owned: HarflowMCP.
- Prerequisite receipts: S05.
- Checklist coverage: C0-MC-05 (final contribution), C0-MA-01 (final contribution), C0-MA-02 (final contribution).
- Scope and acceptance: Vendor the complete canonical bundle and prove matching digests, independent version gates, decoder compatibility and fallback.
- Output: lead receipt `docs/migration-receipts/S06.md`, with the evidence and identities required by the dispatch protocol.

### S07 — Conformance infrastructure

- Status: planned. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S06.
- Checklist coverage: C0-MC-04 (partial).
- Scope and acceptance: Deliver isolated black-box runner, fake harness, external spawn ledger and independent oracles with manifest identity.
- Output: lead receipt `docs/migration-receipts/S07.md`, with the evidence and identities required by the dispatch protocol.

### S08 — Task and authority conformance

- Status: planned. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S07.
- Checklist coverage: C0-MC-04 (partial).
- Scope and acceptance: Run task lifecycle, messaging and approval scenarios against pinned Python, recording external effects, limitations and defects.
- Output: lead receipt `docs/migration-receipts/S08.md`, with the evidence and identities required by the dispatch protocol.

### S09 — Workflow and resource conformance

- Status: planned. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S08.
- Checklist coverage: C0-MC-04 (final contribution).
- Scope and acceptance: Run workflow, resource and statistics conformance for included engine behavior; honor UI removals without treating them as engine API deletion, and assemble the Python baseline receipt.
- Output: lead receipt `docs/migration-receipts/S09.md`, with the evidence and identities required by the dispatch protocol.

### S11 — Client contract boundary

- Status: planned. Lead: HarflowClient. Co-owned: HarflowMCP, HarflowDesign.
- Prerequisite receipts: S09 and S10, with approved references for the assigned states.
- Checklist coverage: C0-CL-01 (final contribution), C0-CL-02 (final contribution), C0-CL-03 (final contribution).
- Scope and acceptance: Deliver v2 models, validators, injected ports, fakes and contract checks traceable to canonical public contracts and exact approved design identities; do not infer authority from mocks.
- Output: lead receipt `docs/migration-receipts/S11.md`, with the evidence and identities required by the dispatch protocol.

### S14 — Python adapter and polling

- Status: planned. Lead: HarflowDesktop. Co-owned: HarflowMCP, HarflowClient.
- Prerequisite receipts: S13.
- Checklist coverage: C0-DE-01 (final contribution), E0-DE-02 (partial).
- Scope and acceptance: Pin the Python runtime and client package; implement public compatibility handshake and bounded polling with capability and uncertainty handling.
- Output: lead receipt `docs/migration-receipts/S14.md`, with the evidence and identities required by the dispatch protocol.

### S24 — Electron on Python acceptance

- Status: planned. Lead: HarflowDesktop. Co-owned: HarflowMCP, HarflowMac, HarflowDesign, HarflowClient.
- Prerequisite receipts: S23.
- Checklist coverage: E0-MC-01 (final contribution), E0-MA-01 (final contribution), E0-DS-02 (final contribution), E0-CL-05 (final contribution), E0-CL-06 (final contribution), E0-CL-07 (final contribution), E0-CL-14 (final contribution), E0-DE-02 (final contribution), E0-DE-06 (final contribution), E0-DE-12 (final contribution).
- Scope and acceptance: Accept Electron on Python against approved v2 production pixels and the retained/simplified feature ledger, including reconnect, disposal, accessibility, performance, clean pinned install and sandbox smoke. Do not restore removed UI for parity; record exact identities and limitations.
- Output: lead receipt `docs/migration-receipts/S24.md`, with the evidence and identities required by the dispatch protocol.

### S39 — Setup and MCP

- Status: planned. Lead: HarflowEngine. Co-owned: HarflowMCP.
- Prerequisite receipts: S38.
- Checklist coverage: R0-EN-07 (final contribution).
- Scope and acceptance: Implement setup, registrations and included MCP tool schemas; verify public behavior with isolated configuration.
- Output: lead receipt `docs/migration-receipts/S39.md`, with the evidence and identities required by the dispatch protocol.

### S41 — Rust conformance acceptance

- Status: planned. Lead: HarflowEngine. Co-owned: HarflowMCP.
- Prerequisite receipts: S40.
- Checklist coverage: R0-EN-08 (final contribution), R0-EN-09 (final contribution), R0-EN-10 (final contribution), R0-EN-11 (final contribution), R0-EN-12 (final contribution), R0-EN-13 (final contribution), R0-EN-14 (final contribution), R0-MC-01 (final contribution).
- Scope and acceptance: Pass retained engine wire and semantic conformance and listed fault families on macOS with external oracles; frontend removals do not delete APIs. Adjudicate discrepancies without claiming other platforms.
- Output: lead receipt `docs/migration-receipts/S41.md`, with the evidence and identities required by the dispatch protocol.

### S46 — Cutover rehearsal

- Status: planned. Lead: HarflowEngine. Co-owned: HarflowMac, HarflowDesktop, HarflowMCP.
- Prerequisite receipts: S45.
- Checklist coverage: I0-EN-03 (final contribution), O0-EN-02 (partial), O0-MA-01 (final contribution), O0-MA-02 (partial), O0-MC-01 (partial).
- Scope and acceptance: Rehearse isolated archive, restore, validated definition/configuration/preference imports, registration backup and handoff; document rollback boundaries, exclusive roots and no history import.
- Output: lead receipt `docs/migration-receipts/S46.md`, with the evidence and identities required by the dispatch protocol.

### S47 — Owner cutover and canaries

- Status: planned. Lead: HarflowDesktop. Co-owned: HarflowEngine, HarflowMac, HarflowMCP.
- Prerequisite receipts: S46.
- Checklist coverage: O0-EN-02 (final contribution), O0-MA-02 (partial), O0-MC-01 (partial).
- Scope and acceptance: Only with explicit owner authorization, settle active work, perform cutover and canaries; retain any qualified early-use next store and record first writes and external effects.
- Output: lead receipt `docs/migration-receipts/S47.md`, with the evidence and identities required by the dispatch protocol.

### S50 — Contract transfer and retirement

- Status: planned. Lead: HarflowMCP. Co-owned: HarflowEngine, HarflowMac, HarflowDesktop, HarflowClient.
- Prerequisite receipts: S49.
- Checklist coverage: O0-MC-01 (final contribution), O0-EN-03 (final contribution), O0-MA-02 (final contribution).
- Scope and acceptance: With explicit owner decisions, transfer canonical contracts once with consumer/provenance updates and retire legacy product; cleanup needs separate authorization and never deletes user repositories implicitly.
- Output: lead receipt `docs/migration-receipts/S50.md`, with the evidence and identities required by the dispatch protocol.
