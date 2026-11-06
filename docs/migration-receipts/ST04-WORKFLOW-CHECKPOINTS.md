# ST04 workflow checkpoint integration

Date: 2026-11-06. Canonical owner: HarflowMCP until accepted O04 transfer.
Status: actual workflow checkpoint conformance implemented with explicit unpaid
fixture scopes. Production poststart control remains unsupported.

## Implemented stage

The [workflow integration](../decisions/ST04-workflow-checkpoints.md) connects the
approved two-request protocol to actual `WorkflowStore`, `WorkflowSupervisor`,
delegation, scheduling, routing/join, resource and reconciliation machinery.
It extends the [detached producer](ST04-POSTSTART-PRODUCER.md); it is not another
standalone transition model and does not register a production adapter.

The embedded ledger and actual workflow activation, task, pending token and normal
attempt charge publish in one authoritative run CAS. Ordinary result acceptance
updates both views atomically; ordinary configuration/grant changes invalidate old
qualification rather than being overwritten by ledger state. Original enrollment
checks the actual declared configuration and accepted Fresh choice after awaits
and again inside the reservation CAS.

Original issuance and successor commands use durable claims and one-use private
delivery. Missing scope, uncertain publication, lost acknowledgment and stale
generation/evidence refuse effects without allocating another identity. All
provider issuance from a fixture-backed run requires its code-held scope, including
later unrelated activations. Persisted synthetic facts cannot authorize a stock
`TaskRegistry`, registry fallback or ordinary provider cancellation.

Actual result, decision, assignment, routing, join, scheduler and recovery guards
protect the controlled source while allowing independent active sibling results.
Only the registered complete outer job can acknowledge quiescence. Positive full
settlement records a certificate-bound logical cancellation of the old workflow
task/activation, without inventing a worker result or changing its `TaskRecord`.
Retained settlement proofs survive later readiness invalidation.

The existing resource ledger atomically transfers the same exact standalone
reservation from old owner to coordinator to successor. No release/grant-pass gap
admits a queued competitor. Current and archived journals bind the actual claim,
names, captured generation and deterministic owners; repair cannot resurrect the
old owner or transfer an unrelated valid-shaped claim. Generic cleanup preserves
controlled claims until their journal permits release.

Shared writer bounds include normal steering and the embedded component. A
workflow-specific completion reserve and conservative admission envelope cover the
remaining certificate, proof, journal and archive copies. Typed completion writes
consume that reservation rather than requiring it a second time. Rejected unrelated
writes may leave a protective adapter fence ahead; explicit bounded observation
reconciles forward without changing authority flags, identity, allowance or issuing
an effect permit. Accepted replay remains available.

## Source and immutable bundles

Input commit: `baabb8473ab88a1f6ba978fc6e4723f769da20be`.

| Bundle | Manifest SHA-256 |
| --- | --- |
| Immutable synthetic poststart candidate | `b5e0949dec6cb5d3d20bf989ea2171a245ef008b77e9b7b8019c8b9eff2e180b` |
| Supported 258-entry producer | `1c63905bf9303bf9dce2b5b5160da33fdac6aeeaf6cf125cd82324dd2f3e948c` |

All contract bundle files and independent owner-runtime component inputs were
byte-identical to the input commit. No supported endpoint, fixture publication,
capability, product version, consumer selection or Rust adoption changed.

## Review and observed verification

Independent plan review closed two findings in two rounds: enforce the resource
handoff through a real atomic primitive and make actual workflow state authoritative
on ordinary mutations. A native Sol low implementer and independent Sol low test
author had disjoint scopes; the primary implemented the dependent runtime/checkpoint
integration, adjudicated findings and independently verified the result.

A fresh read-only source reviewer completed two rounds. Three first-round defects
were fixed: stale configuration across original-launch awaits, perpetual settling
of certified retired originals, and resource journal retargeting. Round two found
no remaining actionable findings; its three focused convergence tests also passed.

Observed offline checks:

- Final guarded standalone collection: **132 tests passed** with frozen inputs:
  58 genuine workflow tests, 45 detached producer tests and 29 unchanged candidate
  tests. The candidate suite includes 240 bounded schedules and 15 canonical traces.
- Separate audited regression collection during integration: **122 tests passed**
  across prestart replanning, retention and resource modules. The long rolling-window
  cases were included. This is not a full product-suite or runtime qualification.
- Exact candidate generation check, 12 owned Python AST checks, maintained local
  links, private-reference/legacy-name guards and staged whitespace checks passed.
- The two preexisting dirty migration documents and three `dist-rc` artifacts were
  preserved by exact hash and excluded from this stage.

Integration opt-in flags were unset. Standalone discovery allowed only the named
unit modules; the selected pytest run used explicit files, a collection guard and
`not integration and not cli_integration`. Helpers used temporary owned roots and
independent atomic synthetic effects. Resource contention tests used only the
audited local Python fixture driver. No live provider, network, vendor registration,
real-user process signal or live state root was exercised.

The [machine verification record](ST04-WORKFLOW-CHECKPOINTS-checks.json) pins the
reviewed inputs, preserved files, independent component and fixture identities.

## Exact remaining gates

The tested first profile is root connected headless/Fresh, with an immutable
assignment, complete fixture domain and standalone exclusive resource claims.
Native/Resume, nested/specialist, unknown worktree ownership, affected additional
providers, unsettled steering responses and borrowed/lent resource topologies
remain explicit refusals. No unsupported case falls back to another graph path,
provider replanner, PID lookup or process-group cancellation.

Ready handoff consists of the canonical policy, immutable candidate bundle, embedded
producer/checkpoint machinery, resource primitive and independent workflow fault
tests. A separately assigned accepting runtime still needs qualified original-launch
OS containment, capture/writer closure, trusted enforced repeat-safety, production
caller/owner authority and measured bounds. Synthetic tests cannot supply those facts.
Only after matching conformance and qualification may a supported ctl capability
and fixtures be proposed. Rust adoption, consumer integration, actual UI/runtime
acceptance, installation and release remain separate assignments. No tracker
acceptance, sibling mutation, push, release, install or acquisition was performed.
