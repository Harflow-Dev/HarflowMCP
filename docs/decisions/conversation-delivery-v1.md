# Conversation delivery v1

The internal `conversation_delivery_v1` document is separate from legacy manual
follow-ups. One Queue message claims one response, FIFO, after an observed native
response boundary. A Now request takes priority and preserves earlier queued
messages. Only one unresolved Now is admitted per source generation. Admission,
result acceptance and closing must share the owning activity's serialization.

Standalone documents use a permanent per-conversation leaf lock under the owned
state root. Workflow owners embed the same document and pure transitions inside
`update_run`; they must not nest this leaf store beneath a second ledger mutation.
No subprocess, native write, callback to another store, or await belongs inside
a store mutation. The store is not a retention driver or a provider adapter.

The logical conversation has an explicit native context and head task, generation
and turn. A fresh continuation does not become its head. Each claim carries exact
source identities. Consumption requires the adapter's correlated native input
receipt and a distinct successor generation, never a running process. Native
ACK, input consumption, response terminal and tool settlement remain distinct.
Owners must reconcile native interruption, tool effects and approvals before
publishing the boundary that permits Now's claim. Public caller booleans must
never be translated into native receipts or boundary evidence.

Persist claims before effects. A failed/partial write or lost ACK becomes unknown;
unknowns block further issuance and never replay automatically. Recovery can
record a subsequently observed matching receipt. It cannot infer consumption
from launch, elapsed time, or process disappearance. Claim replay returns the
same identity without authorizing another effect. Cancellation is preclaim only.

The document is strict UTF-8 JSON, rejects duplicate keys and unknown fields,
and has a 4 MiB read cap. Messages have a 32 KiB cap, at most 32 outstanding
entries and 64 retained final entries. Admission reserves 1 MiB for completion
metadata. Claimed and unknown entries are protected without expiry. The activity
owner must protect the entire document during retention while such entries or
queued input exist. History compaction advances an explicit ledger epoch. New admission requires the
caller-observed epoch; retained exact UUID replays resolve first, and evicted
requests carrying an old epoch refuse. Cumulative issuance survives compaction.

All grants, costs, resource caps, graph holds and draft revisions remain the
activity owner's authority. These transitions do not grant capabilities, reset
budgets, save drafts or accept workflow results. Queue-drain origin is distinct
from fresh automatic continuation and never consumes legacy follow-ups.


Claims reserve the exact destination task, logical turn and generation and advance
the active head atomically before native I/O. The immutable source remains in the
claim. `queue_drain` and `now_delivery` origins distinguish these responses from
fresh automatic continuation. Consumption receipts must match that destination;
spawn and input-accepted acknowledgements do not qualify.

A distinct idempotent promotion control UUID can select an existing queued message
for Now without modifying its original admission fingerprint. Original caller
target and resolved head identities are stored separately, so retries remain bound
to the caller's target even after the conversation advances.

Response accounting is retained outside compacted message history. An internal
trusted response observation deduplicates by generation and records response-local,
native-cumulative or unavailable cost provenance. Native cumulative totals are not
summed. Unknown or mixed-basis usage never becomes complete or silently zero.

Workflow bindings live under the existing steering ledger, guarded by its process
ownership, immutable target and draft/source witnesses. Admission and source result
acceptance share `WorkflowStore.update_run`. The same-activation bridge captures
response history before claiming the successor and returns before ordinary result
parsing. Conversational JSON cannot publish graph results or builder previews.
Nested runs currently refuse admission until a cross-run atomic authority proof is
implemented. Active Now refuses without a qualified interruption owner; the pure
priority transition is not protocol qualification.

Native response completion does not synthesize a process-exit steering receipt.
After positive physical exit and pipe drain, an explicit operator commit request
may name the exact closed conversation, last logical turn and ledger epoch plus
the existing complete steering Open witness. This issues one separately requested
capped same-session response with Fresh refused. It links the delivery binding to
one existing steering episode; it does not mark the old source result accepted.
Builder, worker and orchestrator frames pass their existing role, source, revision,
assignment, budget and positive-exit classifiers before proposing a draft or
advancing the exact graph checkpoint. Profiles without a finite response cap
refuse. A changed native context in the capped response also refuses. Draft Save
remains a separate explicit operation.

Native progress is a bounded observational projection: current generation and
logical turn, starting/issuing/responding/interrupting/waiting-tools/boundary/
unknown/closed phase, native terminal status and pending tool/approval/effect
counts. Only the trusted transport owner writes observations. These fields do not
set a response boundary, establish consumption or settle an OS effect. Source
faults pin the state even before a caller message exists. Token deltas do not
produce metadata writes; phase/count changes do.

The configured `max_turns` limit applies to each explicitly admitted Queue or Now
response. A newly admitted caller message may issue another response under that
same configured limit. Cumulative usage and workflow retry allowances remain
durable and are never reset. The separately requested framed commit response
retains its existing maximum of eight turns.

Workflow candidates select an optional closed native profile explicitly. Ordinary
start and same-session resume preserve the configured cap (including the existing
implicit 100-turn default on capped adapters). A linked commit uses the source
owner's actual qualified profile, including a separately selected Vibe high
profile. Its sealed transport owner validates the prebound prompt, parent context,
cap and issuance and accepts exactly one response, without another queue or public
conversation descriptor. Fresh steering cannot silently discard a selected native
profile. Native-child preference may fall back to the declared headless profile;
it never drops that profile into an unqualified child control transport.

The one MiB retained prose window is shared by legacy steering text and delivery
bindings. Compaction removes only positively settled historical input and response
prose, preserves pending/unknown input and current checkpoints, advances affected
message epochs and records capture gaps. Cumulative accounting, native identity,
activation ownership and explicit commit links survive prose retirement. A bounded
completion reserve is held before admitting input; protected evidence that cannot
fit produces backpressure rather than expiry or replay.

The root retains at most 32 eligible settled response turns and 64 eligible settled
request receipts, counting message, promotion and cancellation UUIDs together with
legacy steering. Protected current checkpoints and unresolved effects are additional.
Older positively settled items can be retired sooner than their per-document count
limit; affected message namespaces advance so expired requests never become new input.

The binding map has no lifetime actor quota. Ordinary actors with no admitted input
do not reserve conversation response prose or completion slots. At most 32 explicit
messages are outstanding across the root; each pending binding reserves 32 KiB of
completion metadata, with bounded response excerpts and full source hashes. Old
accepted, closed actors retire only with their managed task ownership intact and a
persistent closed checkpoint on the original activation. The newest 64 retirement
tombstones and monotonic cumulative accounting expose gaps without freeing unknown
work or permitting old actors to accept input again. Native initial ordinary output
stays in its owned task log; conversation metadata retains its hash when needed.

For 0.0.11, new Now admission and promotion refuse while observed tools, approvals
or effects remain pending: cancellation of those operations is not qualified.
Queue remains available for the next response boundary. If such work starts after
Now admission, the native owner waits until it settles before interrupting; an
uncertain interrupt retains the existing unknown hold and never issues another input.

An initial builder's own authorized preview may refresh its draft witness only in
the successful preview CAS, before any response issuance or pending input. External
draft changes retain the original conflict fence.
