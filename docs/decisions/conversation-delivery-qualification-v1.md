# Internal Mac qualification matrix

Status: owner-approved internal Mac qualification. The independently reviewed
exact profile manifest is populated. Local unit and contract-consumer checks
have passed; package verification, current-head CI and publication are separate
release gates.
This is Python qualification; Rust adoption is independent.

All 28 exact runtime/profile pairs in the companion JSON have observed native
response and physical exit/drain evidence. Queue/Now protocol checks use the
same production driver and durable API; write checks use one isolated native
fixture operation, and closed-context resumes recall an earlier nonce absent
from their new prompt. Deterministic tests cover task, worker, orchestrator and
builder/edit authority bindings and injected faults. This does not claim live
provider execution of every workflow role or OS-level descendant containment.

| Native runtime | Profile | Freedoms | Per-response cap | Context operation |
| --- | --- | --- | --- | --- |
| Claude 2.1.319 | native | read_only, write_in_repo | 8 or 100 | start, resume |
| Vibe 3.1.0 | shipped-medium35-high | read_only, write_in_repo | 8 or 100 | start, resume |
| Codex 0.166.0 | native | read_only, write_in_repo | omitted (unsupported by native transport) | start, resume |
| Antigravity 1.5.1 | native | read_only, write_in_repo | omitted (unsupported by native transport) | start, resume |
| Cursor 2026.10.22-27dcf5f | native | read_only, write_in_repo | omitted (unsupported by native transport) | start, resume |

Scope is the measured Mac runtime binary identity and exact effective profile,
using omitted model/effort/network selections in the measured environment and
managed path roles. Unspecified model selection remains native CLI policy; no
resolved model identity is certified by an omitted selection.
Vibe's declared profile explicitly selects its shipped Mistral medium/high
configuration without changing global settings. Other requested model/effort,
network, approval, command or caller path grants, versions, platforms and caps
are not accepted by inference. They refuse before new input. Native protocol
availability and the advertised delivery capability are not blanket admission.

Now refuses known active tools whose cancellation/closure is unqualified. A
late tool race remains unknown or waits for positive settlement; it never
permits conflicting input. Queue waits for the full response. Unknown usage is
not converted to zero or added as if it were response-local. The original source
and its charged work remain durable across resumes and interruptions.

The companion JSON binds every proposed row to its observed result digest,
frozen implementation snapshot manifest and exact runtime/profile fingerprints.
Its case names identify retained maintainer qualification evidence, not public
contract fixtures. The evidence digest hashes each row without its own digest
field using canonical sorted compact JSON. No raw provider context, prompt,
response, native session identifier, credential or personal path is published.

The owner accepted the scope and only the reviewed exact pairs are populated.
Refusal tests cover other profiles, and the Mac consumer has byte-identical
canonical fixtures with guarded consumer tests. Before release, complete package
verification and current-head CI, and obtain explicit push/prerelease
authorization for the final revision.
