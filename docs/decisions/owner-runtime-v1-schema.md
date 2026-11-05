# Owner-runtime v1 candidate schema

Normative companion to [the design](owner-runtime-v1.md). Synthetic model inputs,
not Python runtime support, authenticated caller evidence or executable requests.

## Common grammar

JSON objects may contain unknown optional fields within the global budgets, but
they cannot replace required fields. All fields listed as required below must be
present. Booleans are never numbers. JSON NaN/infinity, duplicate object keys,
invalid UTF-8, trailing JSON, nesting above16, arrays above64, strings above4096
UTF-8 bytes and documents above24576 bytes are invalid. Integers must be exact.
The outer JSON object has depth0; strings must contain valid Unicode scalars.

| Type | Exact constraint |
| --- | --- |
| `Id` |String matching `[0-9a-f]{32}`. |
| `Digest` |String matching `[0-9a-f]{64}`. |
| `U64` |Integer0..18446744073709551615. |
| `Process` |Object with required `pid` integer2..2147483647, `start_seconds` positive U64, `start_microseconds` integer0..999999, `boot_uuid` lowercase hexadecimal UUID with canonical hyphens. |
| `Root` |Object with required `device` U64, `inode` positive U64, `protocol` integer5. A future protocol is unsupported. |
| `Steering` |Null, or object with required `role` enum `worker`, `builder`, `orchestrator`; `checkpoint` Id; `revision` U64; `issued_attempt` Id. It is context, not approval. |
| `Binding` |Required `operation`, `session`, `reservation`, `attempt`, `generation` Ids; `owner` Process; `root` Root; `request_digest`, `configuration_digest` Digests; `steering` Steering. A non-null steering issued_attempt must equal attempt. |

Every envelope first requires `format` exactly `harflow.owner-runtime.profile`,
`v` U64, `kind` string, `binding` object and `data` object. Wrong format or required
types are invalid envelope. Supported `v` is1 and supported `kind` is one of
`policy`, `permission`, `observation`, `storage`, `steering_guard`. Binding then
undergoes the separate Binding phase. Unknown versions/kinds or
unknown effect-bearing enum values are unsupported. Wrong types or missing fields
are invalid. Known observation enums explicitly include unknown where appropriate.

Every fixture's expected result is an exact JSON object. Invalid/unsupported
documents return only `parse` and `reason`. Apply these phases in order:
preflight byte length (`budget`), strict UTF-8 decoding (`encoding`), bounded
lexical depth preflight (`budget`, counting container/scalar depth outside quoted
strings and honoring backslash escapes), strict JSON/duplicate/nonfinite/scalar
decoding (`encoding`), recursive depth/string/array budgets (`budget`), envelope,
binding, then kind-specific fields. Within each of the last three phases validate
all required structure, types and numeric ranges before checking supported enum
values. Thus future root protocol plus missing owner is invalid binding, not
unsupported. Envelope support failures have reason `envelope`, binding failures
`binding`; kind-specific structural failures have `fields` and unsupported enums
have `enum`. `parse` is `invalid` for malformed/absent values and `unsupported` for
unknown versions/kinds/protocols or effect-bearing enums. Validation always
precedes a model decision.
Valid results have `parse: valid` plus the fields defined for their kind below.

## Policy

Required: `profile_version` integer1. Optional fields and defaults:

| Field | Type/default |
| --- | --- |
| `background_requirement` |Enum `required`, `incidental`; default `required`. |
| `permission_mode` |Enum `os_enforced`, `tool_level`; default `os_enforced`. |
| `background_idle_seconds` |Finite JSON number greater than0 and at most21600; default600. |

Future profile_version is unsupported. Defaults apply only inside an explicitly
selected, otherwise valid profile. The normalized result is:

`{parse: valid, decision: accept, background_requirement, permission_mode,
background_idle_seconds, tool_level_opt_in}`.

`tool_level_opt_in` is true only for an explicit `permission_mode: tool_level`.
An unknown optional flag cannot opt into that mode or override a known field.

## Permission

All fields required:

| Field | Closed type |
| --- | --- |
| `requested_mode` |`os_enforced`, `tool_level` |
| `established_mode` |`os_enforced`, `tool_level`, `unknown` |
| `tool_level_opt_in` |Boolean |
| `qualification` |`passed`, `failed`, `unknown`, `unmeasured` |
| `binary_match`, `configuration_match`, `grants_match`, `protected_paths_match` |Each `proven`, `unknown`, `mismatch` |

Result: `{parse: valid, decision: admit|refuse, reason}`. Check in order:

1. Requested tool_level without opt-in: refuse `opt_in_required`.
2. Any match is mismatch: refuse `qualification_mismatch`.
3. Qualification is not passed, or any match is unknown: refuse `unqualified`.
4. Established mode differs from requested mode: refuse `mode_mismatch`.
5. Otherwise admit `qualified`.

This model covers only the requested filesystem permission boundary. Final runtime
admission independently preserves base freedom, network, publication and exact
command grants. A fixture injects hypothetical trusted qualification-adapter
observations; a public caller cannot submit these fields as evidence. Rust must
refuse stock modes without an actually qualified adapter.

## Observation

All fields required:

| Field | Closed type |
| --- | --- |
| `agent_result` |`unknown`, `succeeded`, `failed` |
| `result_binding` |`matched`, `mismatched`, `unknown` |
| `sticky_failure`, `turn_open` |Booleans; sticky failure belongs to this exact current attempt. |
| `requirement` |`required`, `incidental`, `unknown` |
| `obligations` |`pending`, `satisfied`, `failed`, `unknown` |
| `inventory` |`complete`, `partial`, `unknown` |
| `exit` |`unobserved`, `zero`, `nonzero`, `signal` |
| `stdout`, `stderr` |Each `eof`, `open`, `unknown`, `error` |
| `input` |`reconciled`, `pending`, `unknown`, `error` |
| `capture` |`lossless`, `known_loss`, `unknown`, `error` |
| `watermark` |`complete`, `gap`, `unknown` |
| `containment` |`complete`, `incomplete`, `unknown` |
| `writer` |`settled`, `live`, `unknown` |
| `claims` |`reconciled`, `pending`, `unknown` |
| `execution` |`healthy`, `failed`, `cancelled`, `unknown` |

Result: `{parse: valid, result, workflow, release, reason}`. `result` is unknown,
succeeded or failed. `workflow` is blocked, succeeded, failed or cancelled.
`release` is blocked or evidence_complete; the latter is an observation of satisfied
evidence prerequisites, never a release grant or an operation.

Compute display result in order: sticky_failure means failed; otherwise mismatched/
unknown result_binding means unknown; otherwise agent_result failed stays failed;
otherwise turn_open means unknown; otherwise use agent_result.

Compute workflow disposition in this exact order:

1. Exit unobserved, either stream not eof, input not reconciled, capture not
   lossless, watermark not complete, writer not settled or turn_open: blocked,
   reason `unsettled`.
2. Inventory not complete, obligations unknown, or obligations pending while
   requirement is required/unknown: blocked, reason `obligations`.
3. Execution unknown: blocked, reason `execution_unknown`.
4. Execution cancelled: cancelled, reason `cancelled`.
5. Execution failed or exit nonzero/signal: failed, reason `execution_failed`.
6. Display result failed: failed, reason `result_failed`.
7. Requirement required/unknown and obligations failed: failed, reason
   `required_obligation_failed`.
8. Display result succeeded: succeeded, reason `succeeded`.
9. Otherwise blocked, reason `result_unknown`.

Release is evidence_complete only if workflow is not blocked, containment is
complete, claims reconciled, inventory complete and obligations satisfied/failed.
Otherwise it is blocked. Thus incidental pending work can preserve a result and
permit independently qualified workflow settlement, while its resource claims
remain held. Unknown work is never silently classified incidental. Existing
current-attempt/final-watermark and separate control authority remain mandatory.

These model decisions do not write legacy Task.status, publish terminal events,
set Task.done, schedule continuation or free resources.

## Storage exchange

Required `pending` and `ack` objects, `send_state` enum complete/partial/unknown,
`deadline` enum current/expired, and `poisoned` boolean.

`pending` requires: `sequence` positive U64; `operation`, `generation`, `attempt`,
`channel_nonce` Ids; `writer` Process; `binding_digest`, `command_digest` Digests;
`command` enum bootstrap/claim/record_child/create_raw/accept/capture_begin/append/
known_loss/finish/close; `expected_revision` U64; `payload_bytes` integer0..16384.
Only append can carry nonzero payload_bytes. `ack` requires the same sequence,
operation, generation, attempt, channel_nonce, writer, binding_digest and
command_digest, plus `revision` U64/null and `state` enum committed/refused/unknown.
Committed requires a concrete U64 revision; refused/unknown may use null or a
historical revision, which is not interpreted as the current durable revision.

Claim, record_child, accept, append, known_loss and finish expect checked
expected_revision+1; bootstrap, create_raw, capture_begin and close expect the same
revision. This is an exchange model; the actual executor additionally validates
the command's ownership/capture phase and full snapshots using existing fences.
Payload byte counts/digests in specimens are model observations; real binary
codec tests must check actual bytes and lengths independently.

Result: `{parse: valid, decision: advance|retain, reason}`. Check in order:

1. poisoned: retain `poisoned`.
2. send_state not complete: retain `uncertain_send`.
3. deadline expired: retain `late_ack`.
4. Pending operation/generation/attempt differ from Binding, or any matching
   pending/ack identity/sequence/digest differs: retain `identity_mismatch`.
5. Ack state unknown: retain `commit_unknown`; refused: retain `storage_refusal`.
6. For committed only, revision increment overflows or acknowledgment revision is
   not expected: retain `revision_mismatch`.
7. Otherwise advance `acknowledged`.

Advance means only that exact pending storage command was acknowledged. It is not
execution, steering, retry, release or ownership-transfer permission. After any
uncertain exchange, the runtime sets poisoned permanently; feeding a later good
ack with poisoned=true must retain, not restore admission.

## Steering guard

Required `claimed_proof` enum agent_result/storage_ack/observer_attachment and
`requested_action` enum withdraw/retire/replace/release/control. Every such
substitution returns `{parse: valid, decision: refuse, reason:
not_steering_authority}`. No positive steering-control proof is defined by this
component. Exact accepted steering contracts remain separately authoritative.

## Manifest and provenance

`manifest.json` is at most128KiB and requires format
`harflow.owner-runtime.profile.component`, integer `v: 1`, profile
`owner-runtime-v1`, profile_version1, status specified_unimplemented, provenance
synthetic_specification and python_runtime_support=false. `specifications` is a
two-entry list with repository-relative `path` and `sha256` for both normative
documents. `files` is a one-to-one list of at most64 entries with `filename`,
`bytes`, `sha256` and the exact `expected` model result. No absolute source paths
or arbitrary path traversal are permitted.
The expected result is metadata, not embedded runtime authority in each document.
`manifest.sha256` contains exactly the lowercase64-hex hash of the exact canonical
manifest bytes followed by a newline. No public capability
or real-runtime specimen is generated. Reject symlinks, multiply linked files, extra/missing files,
unsafe inventory paths and inconsistent hashes. Unknown schemas cannot silently
be marked accepted by updating expected outputs from the evaluator.
