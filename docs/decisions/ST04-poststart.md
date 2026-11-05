# ST04 poststart containment and replacement

Date:2026-11-05. Canonical owner:HarflowMCP until accepted O04 transfer.
The owner approved this policy and its initial admission/read bounds. This document
specifies a candidate protocol, not an implemented public control capability.
Rust adoption, accepting-runtime conformance, actual runtime qualification and
consumer/release acceptance are independent gates.

## Scope and provenance

The existing prestart producer is recorded in [ST04](../migration-receipts/ST04.md):
implementation `aae15779fff358079b3e9891782dab3b7af52ac1`, receipt
`b14095bb4bb81a1e492954ca74243540b99906be`, supported258-entry manifest
`1c63905bf9303bf9dce2b5b5160da33fdac6aeeaf6cf125cd82324dd2f3e948c`.
Its withdraw-only protocol never escalates into poststart signals after a spawn race.

The candidate component is `contracts/candidates/st04-poststart-v1`, with a separate
manifest and the [closed schema contract](ST04-poststart-schema.md). All examples
and model results are explicitly synthetic and grant no runtime authority. It adds
no supported `harflow.ctl` envelope, advertised capability, product version, backend
route, installed runtime or consumer selection. Candidate schema validity is not
trusted evidence or authorization.

The first profile targets one root connected headless/Fresh worker attempt, enrolled
in a qualified domain at its original launch. It preserves the immutable assignment,
node, selected candidate, goal/definition and grants. Saved PIDs or observer attachment
cannot retrospectively enroll a legacy live process. Nested workflows, Native,
Resume, specialist invocations, ambiguous ownership and mixed unsupported domains
refuse before effects. An additional active steering provider is outside the first
single-provider profile; a queued response can be revoked only with positive
no-issuance evidence. Future affected provider/descendant issuance is fenced.

The stop domain must include every participating managed descendant, OS process,
capture actor and storage writer, and remain closed against future creation until
settlement. A headless label, process group, one-time enumeration, empty survivor
list or best-effort cancellation cascade is insufficient. Unknown or over-budget
inventory cannot be truncated into completeness.

## Two independent explicit decisions

1. **Contain.** Authenticate the qualified external caller or verified human,
   independently authorize the exact cancellation scope, and durably record intent
   and fences before any stop effect. This authorizes no replacement or model turn.
2. **Replace.** After safe settlement, accept another explicit request bound to
   the exact certificate, trusted effect disposition and fresh authority/checkpoint
   witnesses. Validate the closed immutable replacement subset before recording a
   deterministic linked control decision, attempt and launch identity.

A ready observation grants neither step. Managed workers cannot self-authorize;
a question or approval cannot authorize its own answer. Existing interaction-owner,
verified-human takeover and cancellation eligibility remain independent checks.
Native clients continue to use ctl only; no MCP or private-state fallback is added.

The replacement decision has its own control ledger and is not an unrestricted
ordinary orchestrator frame. No provider/model replanner runs in this first profile.
Reject assignment edits, checklist/task updates, optional skips, graph selection,
root completion, goal operations and backend/session changes before mutation.
The original accepted orchestrator decision stays accepted. Work needing a different
assignment or route requires a separately authorized ordinary operation and later
control design; it cannot silently widen this request.

## Ordering, settlement and uncertainty

Admission CAS binds the root/activation/task/attempt/visit, immutable assignment and
configuration, definition/goal revisions, interaction/control/target revisions,
caller/grant evidence, original owner/process/domain generations, exact scope,
qualified adapter/profile identity, reason and idempotency identity.

An already accepted normal result wins a stale request. If containment intent wins,
fence affected result acceptance, routing/join contributions and new issuance before
signals. Later original output remains historical and cannot finish a successor.
Preserve independent sibling results, accepted checklist/decisions, artifacts and
partial effects. Ambiguous dependency overlap refuses; whole-root cancellation is
not a shortcut for one scoped stop.

Durable intent/fence publication is an atomic storage CAS. Do not hold control
coordination/tree locks across signals, waits, process probes or blocking storage
RPC. The storage adapter owns its transaction/commit discipline; an uncertain
commit acknowledgment authorizes no effect. The owner-loop storage isolation and
nonpreemptible I/O limits remain explicit. A qualified adapter uses captured original
handles/generations, never a newly
looked-up pgid. Existing ordinary cancellation remains separate; unsupported control
does not fall back to it. Persist delivery intent before the effect. Lost delivery
acknowledgment is unknown and never automatically resends a stop.

The phases distinguish cancellation requested, settling, settlement unknown, ready
to replan and replacement committed. Missing, contradictory, stale or exhausted
proof protects exclusions and reports attention. Polling/reconnect cannot renew an
execution budget, grant a control action, relaunch an owner or recreate a successor.
An owner or writer that is live or unknown cannot be adopted through this protocol.

A trusted accepting adapter must establish every independent prerequisite:

- Exact source/intent/domain binding, complete inventory and closure of future
  issuance, with no escaped or unaccounted participant.
- Observed exit/reap as required by the profile; both output streams at EOF, complete
  drainage and input reconciliation, lossless capture and a complete final watermark.
- Every old-domain obligation positively satisfied or failed, including incidental
  obligations. Incidental pending work may preserve a displayed result, never
  replacement or release permission. Unknown is not incidental or empty.
- Every old storage writer settled, acknowledged extents reconciled, no unresolved
  mutation and original resource/pipe/lease obligations closed. An acknowledgment
  alone does not prove writer settlement, capture or control permission.
- Resource/session/message/worktree claims reconciled through an idempotent journal,
  plus whole old dispatcher-job quiescence or exact positive dead-generation proof.
- Permanent stale-result/callback/writer fences and original task-ID retirement.

Exit, successful result, terminal record, owner death, transcript recovery, timeout,
manual resource release or an empty error list cannot substitute for those facts.
Failed/cancelled results remain historical even when the domain later settles safely.
Known capture loss remains blocked under this first profile; there is no risk waiver.

## Effects, replacement and handoff

Repeat-safety needs a named, versioned trusted verifier and an enforced profile
covering the old effects and successor execution. A caller description, model opinion
or local diff is not proof. Positive profiles may prove enforced effect absence, or
reconcile receipt-backed logical effect identifiers with enforced idempotency.
Unknown external effects and unclassified obligations block replacement. A new
operation/grant requires its own authority path, not acknowledgment of this risk.

Before dispatch, reread current authority, exact certificate/effect-verifier binding,
configuration, budget headroom and compatible exclusions. Atomically publish the
new control decision/attempt, supersedes linkage and one planned task identity before
first issuance. An unknown/requested successor launch never gets a fresh identity.
Original task IDs stay retired in every execution mode. Exactly one successor issuance
and no overlapping conflicting execution are protocol invariants to prove; generic
AI effect non-repetition is not promised.

Old execution grants/obligations must settle and release journal correctly. A distinct
coordinator-owned replacement exclusion protects the proof-bound worktree/effect
state until compatible successor claims are acquired. It is not a live old execution
grant; unknown old claims cannot transfer. Handoff must avoid both overlap and waiting
behind its own untransferred hold. External mutation outside a qualified threat
boundary cannot be represented as enforced exclusion.

The issued original attempt remains charged. A successor consumes remaining ordinary
node-attempt allowance; all configured permissions, tools, resources and overall cost
limits still apply. No refund/reset, missing usage as zero or automatic new allowance
is permitted. No model decision is issued, so model-decision counters are unchanged;
explicit control issuance has its own durable accounting.

## Bounds, retention and capacity

Approved initial bounds are128KiB per request/certificate,100 actor/inventory entries,
8MiB shared proof-read work, one unresolved poststart intent per root,4MiB inline
control metadata and1MiB text. The schema contract closes lexical/depth/per-step
bounds. These are bounded work/admission limits, not lifetime issuance-count quotas.
Production stop/grace/execution/background times must be supplied by the configured,
qualified profile. Unknown/absent values refuse. No helper8/4/10-second budget is a
production default; no universal bounded kernel I/O or process termination is claimed.

The prestart32/64 history approval does not expire issued-effect evidence. Initially
retain poststart intents, effects, acceptance/ownership fences, required certificates
and cumulative accounting. Coalesce bounded repeated observations. Admission reserves
completion/journal capacity for an already accepted intent. On capacity exhaustion,
refuse new controls before effects while keeping accepted-intent observation and
exact replay available. Further poststart admissions can remain blocked until a
separately approved archival/compaction policy exists. No unresolved evidence expires
because of age, timeout or an observer disconnect.

## Delivery and independent gates

Canonical work first delivers this specification, closed candidate schemas, a pure
fault model, synthetic specimens and independent deterministic oracles. No runtime
module may import this tooling as an authority adapter. Hypothetical model facts
never reconstruct a production admission token from JSON or a persisted report.

Rust adoption requires a separate assignment, accepted canonical subset and exact
commit/manifest pin, followed by independent Rust conformance. The private
owner-runtime candidate and supported0.0.10/full207 baseline remain separate;
neither implicitly adopts the steering258 producer or this new candidate.

Actual runtime qualification independently needs the precise backend/version,
executable/configuration/grants, caller/domain authority, OS/filesystem/topology,
effect verifier, capture/writer faults and production bounds. Alias/pre-open-handle
and future-descendant behavior are separate evidence. Supporting measurements may
proceed under their own authorization in parallel; diagnostics or synthetic tests
cannot advertise poststart support. See [owner-runtime](owner-runtime-v1.md) and
[backend qualification](backend-enforcement-qualification-v1.md).

Public capability/fixture publication requires an accepting implementation and matching
qualified profile. Consumer adoption, byte-identical fixture vendoring, selected
engine/version gates, actual UI verification, installation and release remain
separately assigned. No push, live provider or release is authorized by this component.
