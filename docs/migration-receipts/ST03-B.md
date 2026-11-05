# ST03-B — Worker conversational steering

Date: 2026-11-05 (Asia/Jakarta). Lead: HarflowMCP.
Status: implemented and independently reviewed; all 1,022 selected unit checks demonstrated passing
on unchanged source across the initial run and a scoped environment rerun. No owner acceptance,
release or consumer adoption.

## Assignment and inputs

The owner authorized ST03-B in the existing steering session, including cohesive local commits.
This supersedes the ST03-A stop only for worker steering and its approved shared retention policy.
ST05/ST06, sibling changes, installation, registration, live providers, paid probes, push,
publication and release remain outside this assignment. The primary checkout is used; no branch
or worktree is created. Tracker-owned migration documents and existing `dist-rc/` files are excluded.

Input HEAD: `6b32c331b06752c4b5e89bcff8798772121b1cde`; prior implementation
`7c527d8fe060ed00272d6207a4cecfc8bf5b2aef`; source tree
`95a81192756f2caa4642d1e46ebd25b9144bc4a3`. Input producer bundle: 235 files,
manifest SHA256 `66e06af29b195472e42215676e8feb1028265c280a7aa08e5f54f39a55e36cd5`.
Historical [ST02](ST02.md) and [ST03-A](ST03-A.md) receipts remain unchanged.

## Approved policy and implementation

The owner replaced fixed steering-count quotas with unlimited explicit messages. Each message
authorizes one finite provider response, with no automatic conversation, retry, fallback or repair.
The original worker activation, normal attempt count and execution allowance are neither consumed
nor reset by conversational responses. A fresh response has its own task, issuance accounting and
resource reservation, preserving the original configured permissions, filesystem/network controls,
approvals and provider settings. No generic workflow hard dollar cap exists or is promised.

Worker targets bind the root and exact reciprocal invocation path, activation, original task,
immutable assignment, definition, goal, interaction/control revisions and specialist checkpoint.
Admission and normal result acceptance share the root mutation lock. Active original work is
observed at its exact process boundary before a response can issue; unknown observation never
means permission to replace a task. A durable `worker_result_required` blocker prevents dismissal
or ordinary recovery from replenishing the original execution allowance.

Prose waits for the caller. A whole framed command passes the original worker result validator
and commits once into the same activation. Successful result acceptance releases only the
steering-created pause and resumes the configured graph. Connected worker questions retain their
real question identity and ordinary orchestrator-answer budget. Answer delivery needs another
explicit finite response. Specialist results use the original call settlement/delivery receipts;
specialist `asking`, Native steering, unqualified Resume and adapters without a finite cap refuse.
Fresh context requires explicit `allow_fresh_context` consent; the response cap is at most eight
harness turns, honoring a smaller configured positive cap.

The owner separately approved a rolling shared history window: 32 positively settled turns,
64 settled request receipts, 1 MiB conversation text and 4 MiB inline metadata. These are retention
windows, not lifetime issuance limits. Protected in-flight/unknown claims, unresolved effects and
required session checkpoints remain; protected storage pressure refuses before spawning. Cumulative
issuance and observed usage survive retirement, with unknown cost/turn usage explicitly counted.

An explicit schema-v2 upgrade preserves qualified legacy records. A distinct monotonic storage
epoch closes the request namespace durably before pruning. Retained exact old requests replay;
expired old-epoch requests refuse without dispatch. Reusing a UUID in the current epoch is a new
explicit request. Absolute history cursors expose gaps. Exact old receipts and raw history may
expire. Durable managed-task bindings prevent surviving retired TaskRecords from becoming
standalone resumable tasks. This protocol is additive behind worker/window capabilities; no
consumer adoption or runtime selection is part of this producer change.

Raw provider records and logs expire only through the normal live-server retention policy
(30 days by default), with existing positive settlement, session/task locks, launch, resource,
phase, follow-up, artifact and descendant protections. Disabling retention retains those raw
files. The inline window is not a total disk quota; ctl never starts a sweep. Qualified retired
ownership sidecars survive until raw deletion and their interrupted cleanup retries safely.

The independent review and primary self-review found recovery defects during implementation:
compaction could exceed its durable plan; workflow-attempt resources could remain held after a
recovered original boundary; a digest alone could substitute for the actual worker result;
specialist delivery failures could lose an applied effect claim; generic steering recovery
blocked legacy cancellation; and provider exception types were mistaken for no-spawn proof.
Corrections preserve exact compaction plans, use the normal resource coordinator after positive
observation, qualify recorded effects, retain uncertain delivery claims, observe cancellation
without reconnecting a worker result, and require explicit pre-spawn proof. Additional strict
authority-field qualification and independent known/unknown usage accounting cover persistence
and retirement boundaries. Independent review round 1 closed with no remaining actionable findings after these corrections.

## Verification record

Primary frozen-source run: **1,016 passed, six failed, two deliberately deselected in 663.58 s**.
The six failures concern synthetic owned-process identity/approval pipes under the sandbox; one
returned no identity for the current test process. The exact six tests passed outside the sandbox:
**six passed in 10.16 s**, without code or assertion changes. This is combined evidence across
unchanged source, not a claim that the initial invocation was green. No broader unchanged suite
was repeated. All 785 checked source/script/test/contract file hashes matched before and after
qualification; these include all 192 Python source files.

The fail-closed collection guard observed 1,022 selected cases, both integration flags absent and
zero forbidden markers. The [check inventory](ST03-B-checks.json) records the exact 35-file profile,
excluded tests, initial failures, scoped rerun and source-inventory digest. Selected bodies/helpers
were audited for temporary owned roots, fake registries, and local synthetic process fixtures.
The two live-home/notification-lifespan exclusions remain unchanged. Every pytest invocation used
`-m 'not integration and not cli_integration'` with both flags unset, never set to `0`.

The final steering and handshake generator checks pass. The complete canonical bundle has
**248 files**, manifest SHA256
`5c90aeabf0deabeff93fad75e665460575349aee67e14d7e88870d04020c5c43`.
All 55 non-steering MCP tool schemas are identical to the input; the inventory remains 58 tools.
Private-reference, legacy-name and whitespace guards pass. Five protected original files retain
their captured hashes. Early implementer checks remain diagnostic; the primary post-fix evidence
above qualifies the frozen source. Immutable implementation/source-tree identities follow in the
receipt metadata commit.

All pytest invocations must have both `HARFLOW_INTEGRATION` and `HARFLOW_CLI_INTEGRATION`
unset and explicitly select `-m 'not integration and not cli_integration'`. A fail-closed collection
guard verifies those conditions. New tests use in-process fake providers and temporary owned roots.
The prior audited affected profile is retained, excluding the exact live-home wire-version test and
the MCP notification-lifespan attention test recorded in ST03-A. Additional selected retention and
resource tests are audited before execution. No real provider, installation, vendor configuration,
registration, consumer/UI, CI or release qualification is implied.

The owner stage amendment permits proceeding into ST04 after ST03-B within accepted control and
settlement authority, following independent design review. Unsupported adapter authority remains
a material decision gate. Consumer fixture vendoring, Rust implementation, acceptance, publication
and runtime selection require separately assigned work.
