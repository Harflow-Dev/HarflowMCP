# ST04 poststart candidate schema and fault-model contract

This closes the candidate syntax for the owner-approved [poststart policy](ST04-poststart.md).
It is not a public ctl/MCP command, production certificate or control grant.
The generated files in `contracts/candidates/st04-poststart-v1` are self-contained
Draft2020-12 structural schemas with no external schema references to resolve.

## Envelope and validation layers

Every specimen is a closed object with these common fields:

| Field | Required value |
| --- | --- |
| `format` | `harflow.steering.poststart.candidate` |
| `v` | integer1 |
| `status` | `specified_unimplemented` |
| `provenance` | `synthetic_specification` |
| `python_runtime_support`, `rust_adoption`, `runtime_qualified` | each false |
| `hypothetical` | true |
| `kind` | one of the four kinds below |

Unknown fields reject. Values must be strict UTF-8 JSON, without duplicate keys,
nonfinite numbers or invalid Unicode scalars. The entire document is at most131072
bytes; strings and keys are nonempty and at most4096 UTF-8 bytes; nesting depth is
at most16 from an outer depth of0; each array/object has at most100 entries.
Numbers are finite with absolute value at most9007199254740991. Integer fields
require actual integer tokens and exclude booleans. Reasons additionally require
nonblank content, while preserving the supplied prose verbatim.

Structural schemas describe shape, enums, numeric/character ranges and closed
properties. The bounded decoder adds lexical and semantic rules: UTF-8 byte limits,
strict integer-token typing, keyed actor uniqueness and digest crosslinks. JSON
Schema character length is not UTF-8 byte length, and mathematical integer1.0 may
pass a structural integer rule while the strict decoder rejects its token type.
Neither schema acceptance nor bounded decoding authenticates evidence.

## Closed source identity

`identity` contains exactly:

- Opaque bounded source references: `root`, `activation`, `task`, `attempt`, `visit`,
  `node`, `candidate`, `owner_generation`, `containment_generation`, `process_generation`.
- Canonical lowercase hyphenated UUID: `launch_id`, the original source launch.
- Nonnegative integer witnesses: `goal_revision`, `interaction_revision`,
  `control_revision`, `target_revision`.
- Lowercase64-hex SHA-256 witnesses: `definition_digest`, `goal_digest`,
  `assignment_digest`, `configuration_digest`, `grant_digest`, `profile_digest`,
  `verifier_digest`, `closure_digest`.

These are match witnesses, not caller-created authority or filesystem paths. A
future adapter must resolve typed domain identities and compare authoritative
original enrollment, current scope/grants and profile data. Per-action request IDs
are outside this identity, so the replacement has its own idempotency identity
without changing the original assignment binding.

## Document kinds

`request.schema.json` contains the two closed request variants:

| Kind | Additional required fields |
| --- | --- |
| `contain_request` | `request_id`, `ledger_epoch`, `identity`, `scope`, `profile`, `reason` |
| `replacement_request` | `request_id`, `contain_request_id`, `ledger_epoch`, `identity`, `certificate_digest`, `effect_verifier`, `expected_remaining_attempts` |

Request IDs and containment references are canonical lowercase hyphenated UUIDs;
ledger epochs and expected remaining attempts are nonnegative strict integers.
`scope` is exactly `original_launch_complete_owned_closure`.
`profile` has exactly `name`, `version`, SHA-256 `digest`, and
`expected_time_budgets`; the digest must equal `identity.profile_digest`.
The budget object has exactly positive finite `stop`, `grace`, `execution` and
`background` numeric witnesses. These values are expected configuration, not caller
grants or new production defaults. The accepting runtime must independently match
its qualified profile. There is no public `authorized`, `qualified` or `settled`
boolean and no caller-controlled fixture/production switch.

`effect_verifier` contains exactly `name`, `version` and a64-hex `digest`; the digest
must equal `identity.verifier_digest`. The expected attempt count is a CAS witness,
not permission to mint allowance. Caller-provided assignment/checklist/graph/skip/
completion/session/permission changes are outside the closed shape and reject.

`certificate.schema.json` describes `kind:certificate`, adding exactly
`contain_request_id`, `ledger_epoch`, `identity`, `proofs`, `inventory` and
`effect_verifier`. It is a hypothetical observation object, never a deserialized
trusted adapter token. The proof object has exactly these independent booleans:

`qualified_profile`, `original_enrollment`, `complete_closure`,
`future_issuance_fence`, `exit_reaped`, `stdout_eof`, `stderr_eof`,
`input_reconciled`, `lossless_capture`, `final_watermark`, `writers_quiescent`,
`extents_reconciled`, `handles_settled`, `no_pending_mutation`, `claims_settled`,
`resources_reconciled`, `sessions_reconciled`, `messages_reconciled`,
`release_journal_complete`, `job_quiescent`, `stale_callback_fence`,
`all_obligations_settled`.

Inventory has1–100 closed items with `actor`, `generation`, `obligation` and
`settlement`. Actor IDs are unique. Obligation is `required`, `incidental` or
`unknown`; unknown gets no exemption. Settlement is `satisfied`, `failed`, `pending`
or `unknown`. Every proof must be true and every item satisfied/failed for the
hypothetical evidence-complete classification. An unknown obligation remains
unclassified and blocks that classification even with a satisfied/failed settlement.
Incidental pending still blocks.
An all-true object supplied by a caller has no runtime authority.

`closure_digest` is SHA-256 of the ordered inventory array serialized as compact
JSON with sorted object keys, separators `,` and `:`, UTF-8 Unicode and no added
newline. Array order is binding. The replacement `certificate_digest` instead
hashes the complete canonical certificate document: sorted keys, two-space indent,
UTF-8 Unicode, finite JSON and one final LF. The manifest hashes exact file bytes.
Do not interchange these three digest roles.

`validate_link` checks the closed containment/certificate/replacement triple:
identical source identity and ledger epoch, exact containment request references,
distinct action request IDs and the actual canonical certificate body digest.
It does not authenticate or qualify the certificate, prove its facts or dispatch.
`classify` returns `candidate_valid`, a reason `code`, and constant
`runtime_authorized:false`. Valid request syntax reports `structural_valid_untrusted`;
valid linked syntax reports `structural_link_valid_untrusted`. Incomplete certificate
facts report `settlement_unknown`. No returned object grants stop, replacement or release.

## Scenario grammar and fault model

`scenario.schema.json` describes `kind:scenario` with one `events` array of at most100
closed events. This is a bounded trace of one original lineage, not a whole runtime
or a one-request lifetime quota. A later independently authorized lineage is a
separate model invocation. Arbitrary serialized initial state is not accepted.

Scenario labels `request_id`, `epoch`, `payload` and `identity` are symbolic fixtures;
`identity` denotes the complete immutable source-scope fingerprint, excluding action
request IDs. Real adapters must derive/match complete request and source bindings,
not trust a caller's opaque payload label. Scenario booleans are injected hypothetical
trusted observations, never public request fields or a production token constructor.

| Event | Closed payload besides `kind` |
| --- | --- |
| `admit` | `request_id`, `epoch`, `payload`, `identity`; booleans `authority`, `capacity`, `result_accepted`, `enrolled`, `single_provider`, `scope_exact`, `qualified_profile`, `qualified_budgets` |
| `prepare_stop`, `deliver_stop`, `stop_ack` | none |
| `observe` | exact `proofs` object; booleans `inventory_complete`, `obligations_settled` |
| `certify`, `release` | none |
| `replace` | `request_id`, `epoch`, `payload`, `identity`, strict nonnegative `remaining_attempts`; booleans `authority`, `effect_safe` |
| `prepare_dispatch`, `context_change` | booleans `authority`, `effect_safe`, `budget`, `configuration`, `claims` |
| `deliver_dispatch`, `dispatch_ack` | none |
| `crash`, `recover`, `old_callback`, `accept_result`, `capacity_pressure` | none |
| `sibling_result` | `id` |

Admission persists intent, acceptance/future-issuance fences and completion-space
reservation together, only after current authority, enrollment, profile/budget,
scope and capacity checks. Already accepted result wins. Exact accepted replay
stays stable under later capacity pressure; conflicting bound identity refuses.

The model separates durable command intents from volatile private delivery handoffs
and an independent synthetic external effect count. `prepare_stop` writes an unknown
delivery intent before minting a one-use private handoff; `deliver_stop` consumes it
and increments the external stop ledger. Dispatch uses the same split, after atomic
replacement control/attempt/lineage publication and fresh prerequisite checks.
Crash/recovery loses private handoffs; it never reconstructs one from a durable
unknown intent. Lost acknowledgment cannot resend or allocate another launch ID.
An acknowledgment requires the corresponding observed model effect.

Each new observation advances an evidence revision and invalidates prior certificate,
release eligibility and any pending delivery handoff. Certification records the
exact revision. Incomplete observations, revoked authority, changed configuration/
effects or conflicting claims block replacement/dispatch. Replay preserves original
IDs and charges. Original-result callbacks after fencing cannot mutate successor
attention/session/accounting; accepted siblings remain historical.

Snapshots expose only candidate results: phase, durable journal, effect counts,
unknown/acknowledged statuses, lineage IDs, evidence/certificate revisions, charges,
siblings and refusal/replay verdicts, always `candidate_only:true` and
`runtime_authorized:false`. Model IDs are deterministic symbolic control/attempt/
launch references, not issued runtime task IDs. There is no process, signal, state
root, backend, provider or store implementation in this package.

The trace capacity predicates abstract authoritative runtime accounting. The model
is not a measured allocator, lock implementation or complete production scheduler.
The approved8MiB shared proof read,4MiB inline/1MiB text, completion reservation,
one-pending-intent and permanent issued-effect retention rules remain accepting-
runtime obligations. The standalone tooling additionally bounds individual reads
and generated component size; that does not qualify production memory, I/O or time.

## Canonical component and conformance

The generator owns only this candidate directory. It pins both normative specification
hashes, three structural schemas, exact specimen bytes and full deterministic model
expectations in its own manifest/digest. Every scenario has both classification and
a fixed model snapshot; downstream conformance must compare the expected effect,
lineage, phase and accounting projections, not only JSON validity. Independent tests
check the model against a separately written oracle per event, including cuts before
intent, before/after delivery and before acknowledgments, reordered races, stale
proof/context and callback injection. Parser/structural/semantic rules are tested
separately rather than using the production classifier as their sole oracle.

Generation/check refuse unexpected inventory, observed symlink/hardlink or nonregular
entries and use atomic file replacement. Check is read-only. These are cooperative
artifact-tooling checks, not a hostile-filesystem containment guarantee. The CLI
supports `--check` and an explicitly selected output directory; it imports no engine,
server or backend and performs no runtime control. Supported contracts/manifests,
capabilities and product versions are unchanged. Rust adoption and actual profile
qualification require their separate accepted pins and evidence.
