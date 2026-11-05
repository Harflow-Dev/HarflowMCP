# ST04 durable poststart producer

This internal producer implements the owner-approved two-request protocol in
[ST04-poststart](ST04-poststart.md) using durable synthetic application state.
It does not enable poststart control of a production workflow. No current adapter
qualifies complete original-launch containment, so production admission refuses.

## Transaction and authority boundary

The coordinator keeps the authoritative synthetic target, result-acceptance and
future-issuance fences, source revisions, claims, attempt allowance and control
decisions in one transaction domain. This lets admission race result acceptance
and charge a linked attempt atomically. Independent siblings, accepted checklist
state, original usage and prior decisions are preserved.
Result callbacks must carry their captured complete source identity; after
successor advancement an original callback cannot acquire the successor's identity
by reading the current ledger.

The synthetic adapter implements original-enrollment/profile evidence and an
atomic effect-boundary check of the exact command generation. It cannot replace
the ledger's admission winner or allowance accounting with an in-memory snapshot.
Its facts and effects are conformance evidence only. Caller-supplied candidate
documents, booleans or persisted reports cannot qualify a production adapter.

The producer reuses existing selected-root ownership, storage locking and atomic
fsync replacement. It adds no OS process owner, signal implementation, capture
runtime, resource manager or live-provider route. Creation and reopening are
separate operations with durable enrollment binding; missing established state
refuses rather than starting another ledger.

`poststart_schema` independently validates the closed synthetic documents;
`poststart_store.PoststartStore` owns enrollment and ledger transactions;
`poststart_control.SyntheticCoordinator` applies the protocol. The separate
`poststart_control.poststart_control` production facade always raises
`poststart_unsupported` before reading storage or invoking an adapter. No production
adapter registry or configuration switch is supplied.

## Durable control rules

Containment records exact intent, result/future-issuance fences and completion
capacity before any synthetic stop effect. It grants no replacement. Accepted
normal results win stale admission; later original callbacks cannot advance a
successor. Exact accepted retries preserve their receipt and action identity.

Each command separates durable intent, private one-use delivery, effect and
acknowledgment. Unknown commit or delivery does not recreate a handoff on restart
or permit another identity. Adapter effects run outside storage/tree locks and
must validate the expected generation at their own atomic effect boundary.
Before publishing an invalidating observation or context, the coordinator advances
the adapter's monotonic fence outside storage locks, then performs an expected-version
ledger CAS. If that CAS fails or its acknowledgment is uncertain, the adapter may
remain ahead of the ledger. This deliberately blocks effects; recovery cannot roll
the adapter fence back or reconstruct a delivery capability from persisted JSON.

Every newer observation invalidates old readiness and pending delivery. Readiness
requires the exact source binding, complete inventory, every independent proof
and every classified obligation positively settled, including incidental work.
Release journals old-claim settlement and the separate coordinator exclusion;
it does not grant replacement.

A second explicit request must bind the exact certificate and immutable source,
use a distinct action ID and pass fresh authority, effects, configuration, claims
and budget checks. A deterministic linked control decision and attempt are
published before dispatch. The original attempt stays charged, the successor
consumes remaining ordinary allowance once, and unknown usage remains unknown.
No model decision, assignment edit, goal change or automatic replanner is added.
The accepted request retains its exact containment, certificate and allowance
proof separately from the current observation. Later invalidation cannot erase
that accepted provenance or turn a different payload into an accepted replay.
Deterministic command and successor identities bind the complete enrollment,
root, epoch, source and request, so identical caller IDs in different roots do
not select the same task or command.

After a positively acknowledged successor issuance, an internal enrollment
transition may advance only to that exact deterministic successor, preserving the
immutable assignment and charged remaining allowance. The old intent, receipts,
effects and task retirement remain retained. This permits a later distinct explicit
containment request without turning the one-unresolved-intent bound into a lifetime
count quota. It does not create another task or adopt an unrelated live execution.

Issued-effect evidence and cumulative provenance do not inherit the prestart
32/64 expiry window. Admission applies byte bounds and reserves completion space;
backpressure refuses new controls while preserving accepted replay and observation.
These controls do not impose a lifetime count quota on explicit conversation turns.

## Integration and qualification gates

The private synthetic ledger is not a production workflow record. Real workflow
admission, result acceptance, scheduler/join fences, resource transfer and launch
integration remain undelivered. An accepting runtime must connect those checkpoints
atomically and prove the adapter contract under its precise qualified profile.
The producer cannot be enabled by registering synthetic facts or selecting a flag.

The original candidate bundle remains immutable and synthetic. The supported ctl
and MCP contracts, advertised capabilities and product version remain unchanged.
Rust adoption, actual backend/OS/runtime qualification, consumer integration and
release require their own assignments, accepted pins and evidence.
