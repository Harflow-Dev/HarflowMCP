# Python baseline and public-contract migration checklist

Revised 2026-11-04. Governing [revision 3](../../HARFLOW_MIGRATION_PLAN_V3.md),
[workspace gates](../../HARFLOW_MIGRATION_CHECKLIST.md) and [current sessions](migration-sessions.md).

Keep canonical contracts and independent Python baseline evidence until O04 transfer.
S01/S03/S04(+follow-up)/S05/S07/S08 evidence is delivered with its original limits;
S02/S06 Mac evidence remains separate. S09 bounded evidence is delivered, not accepted.

- [x] C0-MC-01 Bounded public-read implementation delivered in S04/follow-up;
  receipt acceptance/design/release/adoption remain separate.
- [x] B0-MC-03 Bounded source/native baseline qualified in S02, with recorded limits.
- [ ] B0-MC-01/02 Accept inventory and scope dispositions from S01/S03.
- [ ] C0-MC-02/03 Accept S05 handshake/unknown/run-resume/MCP schema evidence.
- [ ] C0-MC-05 Accept S06 fixture/decoder evidence with its bounded failure waiver.
- [ ] C0-MC-04 Complete/adjudicate S07–S09 external-oracle conformance and missing
  faults/runtime closure; synthetic source profiles do not establish full qualification.
- [ ] M03/M12 Support native public-only flows and exact intended 0.0.8 runtime
  qualification; existing 0.0.7 evidence is not 0.0.8 acceptance.
- [ ] R14 Compare Rust conformance against reviewed contract/Python baseline.
- [ ] O04 Transfer canonical ownership once and retire Python only with explicit
  authorization and archive/registration evidence; HarflowMac stays.

S03's exact 47/eight candidate MCP partition and other Proposed/Undecided scope remain
unaccepted. Freeze permits essential maintenance and explicitly assigned contracts.
Use existing development/contract guidance for assigned validation.

## Product engine API

The native app uses **harflow-ctl as its engine API**. MCP is agent-facing only:
no native MCP client or fallback. Existing harflow-setup acquisition/registration
tasks remain separate from product operations. If required app functionality lacks
a ctl operation, first scope additive work in the current canonical contract owner:
HarflowMCP before accepted O04 transfer, HarflowEngine afterward. Specify provenance,
authority, bounds and unknown/refusal semantics, capability, fixtures and tests;
then coordinate byte-identical fixture vendoring and consumer adapters. Pre-transfer
native work starts in Python; Rust implements that same accepted ctl contract.
After transfer, later Web work does not depend on retired Python. Missing API is not
permission to read persistence, invoke agent MCP or invent semantics. M03 may split
bounded suffix sessions.

## Parallel-track coordination

M01 shared readiness follows S09 coverage disposition and the native capability,
code/private-read inventory, with ratified initial ctl/capability/fixture baseline,
exact identities, explicit blocking gaps and canonical change owner. M02+ native
and R01/R02+ Rust work may then run concurrently with disjoint files/isolated roots.
Pin each accepted subset, serialize canonical changes under MCP before O04 (Engine
afterward), coordinate consumer fixture deltas and carry forward affected conformance.
Both accepted M12 and R14 are required before I01; missing baseline/fault evidence
is not automatically waived. Read-only research remains separately assignable.

## Boundaries and evidence

This plan authorizes no implementation by itself. Follow workspace/child/package
AGENTS.md and the current session brief; preserve concurrent work in primary
checkouts. Use isolated roots/fixtures. No implicit live-state, install, registration,
paid-probe, signing, push, publication, retirement or deletion authority. Record exact
commit/contract/design/runtime/profile identities, actual checks/reviews, missing
coverage and owner decisions in the lead receipt. Delivery and acceptance differ.
Builds/tests must not depend on sibling source checkouts. Documentation-only plan
changes need link/consistency checks, not product test reruns.

## Conversational steering scope amendment — 2026-11-05

Canonical ST01–ST04 specification/implementation and producer contracts, with native/Rust consumer handoffs. ST01 alone is currently assigned; exact schemas and later implementation need their own accepted decisions/assignments.
See the [current ST session briefs](../../HARFLOW_MIGRATION_SESSIONS.md#conversational-steering-track--accepted-scope-2026-11-05)
and [steering plan](../../HARFLOW_WORKFLOW_STEERING_PLAN.md). Scope acceptance does
not pass prior gates, permit live control or authorize releases.
