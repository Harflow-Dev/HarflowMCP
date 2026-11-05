# ST04 durable synthetic producer delivery

Date: 2026-11-06. Canonical owner: HarflowMCP until accepted O04 transfer.
Status: private durable producer implemented and tested with synthetic adapters.
Production workflow integration and runtime qualification remain undelivered.

## Delivered behavior

The [producer](../decisions/ST04-poststart-producer.md) implements the existing
owner-approved two-request containment/replacement policies in an actual on-disk
application ledger. It reuses selected-root ownership, workflow storage locks and
atomic fsync replacement. It adds no Python OS/owner runtime.

`poststart_schema`, `poststart_store` and `poststart_control` provide:

- Exclusive enrollment and exact reopening, with initial source, immutable
  assignment/profile, selected root, adapter and allowance bindings. Missing or
  contradictory established state refuses instead of resetting history.
- Atomic synthetic target/result/future-issuance fences and containment admission;
  independent siblings remain unchanged. Complete captured source identity binds
  result callbacks, including after successor advancement.
- Durable command intent, private one-use delivery, acknowledgment and unknown
  outcomes. Effects execute outside storage locks; adapter generation/evidence
  fences prevent delayed stale delivery. Unknown commit or delivery never resends.
- Complete classified obligation settlement, exact certificate/release bindings,
  explicit replacement, deterministic enrollment/source-scoped lineage and one
  normal-attempt charge. Original unknown usage stays unknown; no model turn runs.
- Permanent accepted-request, certificate and allowance proofs, exact replay,
  original task retirement and retained history. Completion capacity is protected
  under metadata/text backpressure. The exact acknowledged successor can become
  the next source without an arbitrary lifetime control-count quota.

The production facade unconditionally raises `poststart_unsupported` before
accessing storage or an adapter. No adapter registration, enabling flag or public
ctl/MCP operation is supplied. Synthetic documents and adapters grant no production
authority.

## Immutable inputs and contract boundaries

Input commit: `9843f9197e00cfdd25bd36625f65a4273681dcb6`.
The original [candidate delivery](ST04-POSTSTART.md) remains unchanged:

| Bundle | SHA-256 of manifest |
| --- | --- |
| Synthetic poststart candidate | `b5e0949dec6cb5d3d20bf989ea2171a245ef008b77e9b7b8019c8b9eff2e180b` |
| Supported 258-entry producer | `1c63905bf9303bf9dce2b5b5160da33fdac6aeeaf6cf125cd82324dd2f3e948c` |

Both complete fixture bundles and every previously tracked production source file
were byte-identical to the input commit. New private source files do not promote
candidate schemas into supported contracts. No advertised capability, version,
native/Rust consumer fixture or runtime selection changed.

## Observed verification and review

Native Sol low implementers had disjoint component and independent-test scopes.
Independent plan review closed two findings in two rounds: authoritative target
state must share the control CAS, and exclusive initialization must be distinct
from reopening. The primary inspected the implementation and ran verification.

A fresh read-only source review completed two rounds. All four first-round defects
were fixed with independent regressions: stale result callbacks, raw versus pretty
JSON byte bounds, permanent accepted replacement proof bindings and cross-root ID
collisions. Round two found no remaining actionable findings.

Final post-fix offline validation passed:

- 45 independent durable-producer unittest methods, covering disk restart and
  before/after commit cuts, lost acknowledgments, reordered/concurrent requests,
  stale effect boundaries, exact source/history/accounting and capacity behavior.
- The unchanged 29 candidate unittest methods, including 240 bounded schedules
  and 15 canonical traces. The explicit collector allowlisted only these two
  modules: 74 tests passed; pytest/conftest and integration tests were not collected.
- Exact candidate generator check, owned Python AST checks, private-reference and
  legacy-name guards, whitespace and maintained local documentation links.
- Exact preservation of the two preexisting dirty migration documents and three
  `dist-rc` artifacts; these files were excluded from this stage.

Integration opt-in flags were unset. Helpers used only temporary owned roots,
synthetic effect ledgers and local fixture generation. No provider, network,
signal, vendor registration or live state-root operation was exercised. No full
product suite or actual runtime qualification is claimed. The
[machine verification record](ST04-POSTSTART-PRODUCER-checks.json) pins the inputs.

## Ready handoff and remaining gates

The durable producer, canonical policy, immutable candidate fixtures and independent
fault tests are ready for a separately assigned accepting-runtime implementation.
That work must connect real workflow admission, result/join/scheduler fences,
original-launch enrollment, resource transfer and dispatch checkpoints atomically.
It must supply a qualified complete containment/capture/writer adapter, trusted
effect verifier and measured production bounds. This detached ledger implements
none of those real workflow checkpoints and cannot be selected as a live engine.

Rust adoption with accepted exact pins, actual backend/OS/runtime qualification,
supported ctl capability publication, consumer integration and release remain
independent gates. Native consumers continue through supported ctl contracts only.
No tracker acceptance, sibling source change, push, release, install or acquisition
is asserted or performed by this stage.
