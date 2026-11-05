# ST04 poststart canonical candidate delivery

Date: 2026-11-05. Canonical owner: HarflowMCP until accepted O04 transfer.
Status: owner-approved specification and synthetic candidate conformance delivered.
Poststart runtime control remains unimplemented and unadvertised.

## Scope and source

The owner explicitly approved the two-request policies and bounds and authorized
the canonical specification, candidate schemas and deterministic fault model.
The [policy](../decisions/ST04-poststart.md) and
[schema/model contract](../decisions/ST04-poststart-schema.md) preserve exact
immutable replacement, original-launch qualified containment, positive settlement
of every obligation, trusted repeat-safety, normal attempt charging and protected
issued-effect evidence with admission backpressure.

The input commit was `a38f643101601e0610d3ad4e54e3bfe6f9742181`.
The production source tree remains `88da8f4787e6e17f66bf013de16a4f4e6ab9633f`;
the supported 258-entry manifest remains
`1c63905bf9303bf9dce2b5b5160da33fdac6aeeaf6cf125cd82324dd2f3e948c`.
The existing [prestart ST04 receipt](ST04.md) is unchanged.

## Delivered candidate

- Separate `contracts/candidates/st04-poststart-v1` component: 54 synthetic
  specimens, three self-contained structural schemas and manifest/digest,
  59 assets totaling 245887 bytes.
- Candidate manifest SHA-256:
  `b5e0949dec6cb5d3d20bf989ea2171a245ef008b77e9b7b8019c8b9eff2e180b`.
  It pins both normative documents, exact inventory/bytes and full model vectors.
- Pure standard-library codec, cross-document binding checks, bounded generator
  and durable-boundary fault model under `scripts/poststart_profile`.
- Independent test oracle, fixed refusal expectations, 15 canonical scenario
  traces and 240 bounded cut/replay and reordered-race schedules.

The model separates durable intent, private delivery handoff, external synthetic
effect counts and acknowledgment. Crash recovery cannot recreate a lost handoff;
new observations invalidate stale proof and pending delivery. The component models
one lineage with injected hypothetical authority/profile facts. It is not a
production allocator, storage implementation, OS containment adapter or scheduler.
All candidate/model results deny runtime authority.

## Review and observed verification

Native Codex component and independent test implementers used Sol at low reasoning.
The primary session inspected the changes and ran the affected standalone tests.
A fresh read-only source reviewer completed two rounds. Round one found malformed
discriminators escaping typed refusal, unknown obligations appearing eligible and
replacement action-ID reuse. All three were fixed with independent regressions;
round two found no remaining actionable defects.

Final post-fix verification:

- 29 standalone unittest methods passed, including the independent schedules and
  trace comparisons; no product pytest/conftest or backend imports were involved.
- Exact generated asset check, reproducibility tests, nine owned Python AST parses,
  normative local links, private-reference and legacy-name guards passed.
- All 459 tracked production-source and supported-contract files were byte-identical
  to the input commit. The five preexisting dirty migration/artifact files were
  preserved by exact hash and excluded from this stage.
- Integration opt-in flags were unset for validation. No live providers ran.

Commands used the repository virtual environment:
`python -m unittest discover -s tests -p test_poststart_profile_fixtures.py` and
`python scripts/generate-poststart-profile-fixtures.py --check`.
The [machine check record](ST04-POSTSTART-checks.json) pins the qualified inputs.

## Remaining independent gates

Accepting-runtime implementation and conformance, explicit Rust adoption with exact
accepted pins, actual backend/OS/profile qualification, consumer integration and
release each remain separate. Synthetic facts, schemas and these tests pass none
of those gates. No supported envelope, advertised capability, product version,
installed runtime or consumer selection changed. No push, install or release was
performed, and no tracker acceptance is asserted.
