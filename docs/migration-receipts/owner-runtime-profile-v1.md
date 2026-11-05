# Owner-runtime profile v1 — canonical specification component

2026-11-05. Status: reviewed definition and synthetic assets verified;
not Python runtime implementation or production owner-runtime acceptance.
Entry HEAD:111fa3ab3a0ce60e1c7c766b1e0e9928c412511b.

## Owner decisions and scope

The owner approved all four recommendations: retain-and-attend unresolved
ownership/capture/containment; separate result, required/incidental/unknown
obligations and resource settlement with unknown required and failures preserved;
strict measured OS enforcement with unsupported refusal and explicit separate
tool-level opt-in; and per-operation storage isolation for owner responsiveness
without a central service or universal bounded shutdown/kill promise.

The owner clarified that Python steering bridges the Mac migration and Rust is
the destination for those accepted contracts. This component therefore establishes
canonical specification assets only. Runtime implementation proceeds in Rust in
the same cohesive stage. No duplicate Python owner/persistence machinery is added.

The release freeze remains: no push, release, installation, acquisition,
registration or live-provider work. No automatic execution-owner adoption,
replacement, replay, release or new cancellation authority is introduced.

## Definition and provenance

- [Production design](../decisions/owner-runtime-v1.md).
- [Exact candidate schema and decision tables](../decisions/owner-runtime-v1-schema.md).
- Independent component: `contracts/candidates/owner-runtime-v1/`.
- Manifest status: `specified_unimplemented`; provenance: `synthetic_specification`;
  `python_runtime_support: false`.
- 64 specimens, 66 component files including manifest and digest. Specimens include
  valid-but-blocked observations, malformed/unsupported input and prohibited
  substitutions for steering/control authority. They are not captured ctl responses.
- Manifest SHA256:
  7df0c93a17b185620e35fcf0d0a3e060174ff0445318ed505dc7e4573a08c6d6.

The separate component preserves supported `contracts/v1`, its aggregate digest,
MCP tool schema and capability advertisement. Existing Python behavior, version
and runtime entry points are not changed by this delivery. Active ST04 changes in
those surfaces belong to their concurrent owner and are excluded from this commit.

Rust must pin this component's exact local source commit and digest independently
from its supported0.0.10/full207 baseline. This component does not adopt the current
aggregate steering fixture bundle, update Mac fixtures or select a runtime.

## Review and implementation

Primary prepared the design; fresh independent pre-code review found and resolved:
exact field/enum/output definitions, durable static policy inside the existing
digest-covered prepared payload, nullable refused/unknown acknowledgment revisions,
shape-before-support validation precedence, both-child/reader/FIFO/IPC teardown,
partial-TX input closure and per-step/stderr bounds. Learned-later writer identity
remains separate deny-only provenance. No additional product decision was needed.

Sol low implemented the standard-library generator/checker and candidate corpus.
Dedicated Luna medium owned the standalone unittest module. No fallback or nested
Harflow workflow ran; usage comparisons were unavailable. The primary owns
adjudication, independent checks and local delivery.

The generator imports no product/server/backend, executes no provider/subprocess,
and accesses no state root. Checking is read-only and validates deterministic
bytes, independent literal case expectations, both specification hashes, exact
bounded inventory and manifest digests. Lazy scandir stops at the expected count
plus one; it does not materialize an arbitrary directory listing. Unicode/JSON,
depth/size, scalar and support classification are specified before model decisions.

Primary self-review corrected envelope type precedence and bounded inventory,
added depth preflight independent of JSON-library recursion limits, isolated the
forged-manifest test from a prior missing-file refusal, and replaced a whitespace-
only oversize negative specimen without weakening its24577-byte budget refusal.

## Evidence and limits

Fresh source review found two concrete issues: unknown optional writer metadata
incorrectly changed identity matching, and in-place generation could truncate an
outside hardlink. Both were accepted and fixed. Comparison now uses the four
validated process identity fields; generation rejects multiply linked targets and
atomically replaces owned entries. Dedicated regressions prove both fixes and
check inode/mtime/mode/size stability during read-only checking.

Scoped checks are deliberately independent of the active ST04
pytest tree and its unmarked live-root/CLI risks; no broad product suite is run for
this disjoint specification component.

The ten standalone unittest methods exercise deterministic generation, hashes,
read-only checking, unsafe inventory/path/link refusal, all five model kinds,
manual expected decisions, required missing/future/unknown states, deep/escaped
JSON, nullable acknowledgment revisions and lazy bounded inventory traversal.
Fixture expectations are authored separately from the classifier; checking does
not learn or rewrite expected results from implementation output.

Synthetic qualified permission observations do not establish a sandbox or admit
a stock backend. A model storage acknowledgment does not establish actual commit,
root exclusion or writer termination. Result/obligation observations do not grant
steering, withdrawal, replacement, release or control. Physical runtime evidence
belongs to the Rust implementation and later actual qualification.

## Concurrent ownership

The owner explicitly authorized coordination with the active steering chat. It
confirmed the disjoint component can proceed with a path-limited local commit,
while reserving runtime/transports, supported manifests, existing generators and
steering documents. Report the exact component commit and end of writes so ST04
can keep its source inventory separate. Preserve its index/work and both repos'
tracker-owned migration plan/session files. No push or release follows this receipt.


## Final primary verification

Local Python3.11.15; no product suite, backend CLI, state root, provider, install or
publication is invoked. Frozen seven-file tooling/test SHA256, sorted
path+NUL+bytes+NUL:
49e908a498ec10e5a95586fc96867309b6a3c3ad3802d503279a6c72ea48ed0f.

| Check | Observed result |
| --- | --- |
| Standalone generator --check |Pass; exact66-file inventory and both normative specification hashes. |
| Standalone unittest discovery for test_owner_runtime_profile_fixtures.py |10 methods passed after final review-driven fixes. |
| Corpus model cases |64 independent literal expectations:40 valid observations/decisions,19 invalid,5 unsupported. These are not64 real-runtime tests. |
| Private-reference / legacy-name guards |Pass. |
| Python AST parsing, local documentation links and scoped whitespace |Pass across owned files; oversize negative specimen retains24577 bytes without whitespace-guard failure. |
| Supported Python surface / release |No runtime/capability/version/supported-manifest change from this component; no push/release. |

The initial oversize whitespace specimen failed the artifact whitespace guard; its
replacement preserves the exact invalid-budget oracle. A test initially used a
macOS-only temp path; it now canonicalizes the standard temporary root rather than
skipping another platform. Local tests do not establish hosted CI or Windows
qualification. Normative synthetic evidence remains distinct from measured runtime
behavior. No additional policy decision or acceptance is inferred.
