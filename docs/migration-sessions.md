# HarflowMCP migration sessions — native revision 3

Revised 2026-11-04. The [workspace session index](../../HARFLOW_MIGRATION_SESSIONS.md)
is authoritative for each brief, prerequisites, acceptance and receipt output.
Read [repository checklist](migration-plan.md) and applicable guidance before work.

## Current ownership

M03/M12 public contracts/runtime support, R12/R14 conformance and O04 transfer; S09 evidence is delivered; acceptance remains pending.

- **O04 — Contract transfer and Python retirement**; prerequisites and acceptance in the workspace index.

Each assignment is bounded and stops before the next. Record its lead receipt in
`docs/migration-receipts/<ID>.md`; co-owned work is included in that lead receipt.
No plan status authorizes live operations or publication. All new sessions are planned.

## Historical session evidence

S01–S09 keep their original IDs, outputs and recorded limits. S09 bounded evidence
is delivered, not accepted. Historical future S10–S50/X01–X03 are superseded and cannot be dispatched;
see the [crosswalk](../../HARFLOW_MIGRATION_PLAN_V3.md#supersession-and-scope-crosswalk).
The preserved briefs below describe original scope, not new authorization.

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

- Status: Partial infrastructure evidence delivered; receipt acceptance pending. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S06.
- Checklist coverage: C0-MC-04 (partial).
- Scope and acceptance: Deliver isolated black-box runner, fake harness, external spawn ledger and independent oracles with manifest identity.
- Output: lead receipt `docs/migration-receipts/S07.md`, with the evidence and identities required by the dispatch protocol.

### S08 — Task and authority conformance

- Status: evidence delivered; owner acceptance pending. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S07.
- Checklist coverage: C0-MC-04 (partial).
- Scope and acceptance: Run task lifecycle, messaging and approval scenarios against pinned Python, recording external effects, limitations and defects.
- Output: lead receipt `docs/migration-receipts/S08.md`, with the evidence and identities required by the dispatch protocol.
- Delivered: implementation `125d37d38b038259c6f0302ce123e708411d50c3`,
  [receipt](migration-receipts/S08.md) and [checks](migration-receipts/S08-checks.json).
  Final primary 198 self-checks and all 9 controlled source task/authority cases
  passed; retained validation rechecked 164 command captures and registered exits.
  Source remains 0.0.7 at `95b696e` with existing Python 3.11.15; no managed runtime,
  real backend, Rust or full C0 claim. C0-MC-04 remains partial. Mac freeze waiver
  preserved; S09 in progress. All changes local only.

### S09 — Workflow and resource conformance

- Status: in progress; receipt and acceptance pending. Lead: HarflowMCP. Co-owned: None.
- Prerequisite receipts: S08.
- Checklist coverage: C0-MC-04 (final contribution).
- Scope and acceptance: Run workflow, resource and statistics conformance for included engine behavior; honor UI removals without treating them as engine API deletion, and assemble the Python baseline receipt.
- Output: lead receipt `docs/migration-receipts/S09.md`, with the evidence and identities required by the dispatch protocol.

## Conversational steering scope amendment — 2026-11-05

Canonical ST01–ST04 specification/implementation and producer contracts, with native/Rust consumer handoffs. ST01 alone is currently assigned; exact schemas and later implementation need their own accepted decisions/assignments.
See the [current ST session briefs](../../HARFLOW_MIGRATION_SESSIONS.md#conversational-steering-track--accepted-scope-2026-11-05)
and [steering plan](../../HARFLOW_WORKFLOW_STEERING_PLAN.md). Scope acceptance does
not pass prior gates, permit live control or authorize releases.
