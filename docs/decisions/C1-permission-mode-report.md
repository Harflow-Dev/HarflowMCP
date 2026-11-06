# C1 candidate: permission-mode reporting and tool-level evidence

Candidate `permission-mode-report-v1`, reserved capability `permission-mode-report.v1`. Implements plan
decision **D6** (approved; it also confirms the explicit opt-in that owner-runtime decision 3 requires).
Specified, unimplemented and **not advertised**; `contracts/v1`, `version-info` and Python behaviour are
unchanged. Assets: `contracts/candidates/permission-mode-report-v1/`. It extends the model of
[owner-runtime-v1](owner-runtime-v1.md) ("Permission admission") with the public report and the
evidence-binding check.

## Report (`kind: report`)

An additive `permission` object inside the existing `enforcement` block, next to the existing booleans:

| Field | Values |
| --- | --- |
| `requested_mode` | `os_enforced`, `tool_level` |
| `established_mode` | `os_enforced`, `tool_level`, `unknown` |
| `tool_level_opt_in` | boolean |
| `opt_in_source` | `none`, `explicit_request`, `freedom_level_d6` |
| `evidence` | `{state, binary_sha256, version, platform, probe_revision, grants_digest}`, `state` in `passed`, `failed`, `unknown`, `unmeasured` |
| `ambient_configuration` | always `uncontrolled` |
| `inherited_rules_may_change_behaviour` | always `true` (the #120 disclosure) |

Invariants (a report violating one is invalid):

- `enforcement.os_enforced` is true **only** when `established_mode` is `os_enforced` and the evidence
  state is `passed` (the "enforcement must never overclaim" rule). A tool-level or unknown mode, or
  unqualified evidence, reports `os_enforced: false`.
- A tool-level request requires `tool_level_opt_in: true`, and `opt_in_source` is `none` exactly when it is
  false. On stock Claude, the 0.0.11 freedom levels count as the explicit opt-in (`freedom_level_d6`),
  per D6.
- Ambient user configuration is disclosed as uncontrolled and is **not** part of the evidence binding.

## Admission (`kind: admission`)

Evidence is bound to the binary digest, version, platform, probe revision and exact grants. At launch,
resume and continuation the check runs before effects, in this order:

1. tool-level request without opt-in: refuse `opt_in_required`;
2. strict (`os_enforced`) request with evidence not `passed`: refuse `strict_unmeasured` (no fallback to
   tool-level or unrestricted);
3. any bound field differs from the current value: refuse `evidence_stale`;
4. evidence not `passed`: refuse `unqualified`;
5. otherwise admit `qualified`.

Caller-supplied JSON is never evidence: the manifest states the specimens are hypothetical
trusted-adapter observations.

## Specimens and checks

Same envelope and bounds as the other C1 candidates; the checker recomputes the admission decision from
hand-written inputs and rejects over-claiming reports, missing disclosure and admissions of stale or
unmeasured evidence.

## Not decided here

- Defaults for non-Claude backends (Codex has an OS sandbox in Python; whether Rust establishes it
  through the same evidence is for that backend's qualification).
- The exact wire vocabulary (`opt_in_source`, `ambient_configuration` values, field names) is proposed
  here and needs owner confirmation before it can be advertised.
- Python's existing `os_enforced` booleans were produced without qualification evidence; this candidate
  does not reinterpret them.
