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
| `evidence` | `{state, mode, binary_sha256, version, platform, probe_revision, grants_digest}`, `state` in `passed`, `failed`, `unknown`, `unmeasured`; `mode` is the one mode this evidence qualifies |
| `ambient_configuration` | always `uncontrolled` |
| `inherited_rules_may_change_behaviour` | always `true` (the #120 disclosure) |

Invariants, checked in this order (a report violating one is invalid):

1. **No silent fallback.** `established_mode` equals `requested_mode` or is `unknown`; a request for one mode
   that runs in the other is `fallback`.
2. **Qualified, for that mode.** Any `established_mode` other than `unknown` needs evidence whose `state` is
   `passed` **and** whose `mode` equals it; otherwise `unqualified_mode`. Tool-level evidence never supports an
   OS-enforced report, and the reverse.
3. `enforcement.os_enforced` is true exactly when `established_mode` is `os_enforced` (and so qualified); a
   tool-level or unknown mode reports `false`, and an established OS boundary reporting `false` is also
   `overclaim` (the report must not understate or overstate).
- A tool-level request requires `tool_level_opt_in: true`, and `opt_in_source` is `none` exactly when it is
  false. On stock Claude, the 0.0.11 freedom levels count as the explicit opt-in (`freedom_level_d6`),
  per D6.
- Ambient user configuration is disclosed as uncontrolled and is **not** part of the evidence binding.

## Admission (`kind: admission`)

Evidence is bound to the binary digest, version, platform, probe revision and exact grants. At launch,
resume and continuation the check runs before effects, in this order:

1. tool-level request without opt-in: refuse `opt_in_required`;
2. evidence `mode` differs from the requested mode: refuse `evidence_mode_mismatch` (evidence for the other
   mode is not evidence; this is what stops tool-level evidence admitting an OS-enforced request);
3. strict (`os_enforced`) request with evidence not `passed`: refuse `strict_unmeasured` (no fallback to
   tool-level or unrestricted);
4. any bound field differs from the current value: refuse `evidence_stale`;
5. evidence not `passed`: refuse `unqualified`;
6. otherwise admit `qualified`.

The binding is exactly binary digest, version, platform, probe revision and grants digest. Optional
`ambient` fields on the evidence and on the current observation (`user_config_sha256`,
`inherited_rules_digest`) are validated but **never compared**: a specimen whose ambient values differ from
qualification time is still admitted.

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
