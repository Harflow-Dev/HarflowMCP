# ST04 workflow checkpoint conformance

The owner-approved [poststart policy](ST04-poststart.md) now has a private
workflow integration, beyond the [detached producer](ST04-poststart-producer.md).
It exercises actual workflow checkpoints with explicitly supplied unpaid fixture
adapters. Production poststart control remains unsupported. No stock backend,
OS containment, capture, writer or repeat-safety profile is qualified here.

## One authoritative run transaction

The private `poststart` payload lives inside the workflow run document. Its
enrollment, protocol ledger, source projection, original-launch marker and runtime
journals commit through `WorkflowStore.update_run` with the actual activation,
pending token and normal attempt charge. There is no mirrored sidecar ledger.
Strict qualification checks both the closed payload and its actual workflow links.

Workflow state is authoritative on ordinary mutations too. Normal result acceptance
publishes the actual node result and the ledger's accepted-result digest in the
same CAS. A later containment request loses that race. Ordinary authorized control,
grant or configuration changes while merely enrolled remain workflow changes;
they invalidate the old poststart qualification instead of rewriting enrollment,
retroactively adopting the source or manufacturing allowance.

Containment and replacement remain separate explicit requests. Replacement creates
only the exact immutable assignment's deterministic activation and planned task,
updates the pending-token supersession and consumes one normal attempt in the
same run replacement. It does not call ordinary model decision acceptance.
Accepted request proofs, task retirement and issued effects remain retained.

## Original-launch fixture scope

A nonserializable `FixtureScope` binds the exact selected store, run, independent
fixture registry and adapter objects. A stock `TaskRegistry` cannot be used for
this path. Persisted `synthetic` fields, environment settings and recovered JSON
cannot reconstruct this capability. Without the exact code-held scope, an enrolled
or planned fixture activation refuses before ordinary registry effects.

The genuine `_dispatch` fixture path reserves and enrolls the original task before
its unpaid effect. The declared node configuration, accepted Fresh choice,
assignment, goal, permissions and allowance are revalidated after resource/slot
awaits and inside the enrollment CAS. Durable original issuance and protocol commands have separate
claims and acknowledgments; uncertain publication or delivery never retries an
effect. A protective adapter fence precedes containment publication so an original
issuance already claimed but delayed cannot execute afterward. Current caller and
cancellation authority still use the existing workflow authorization checks.

The initial conformance profile covers one root connected headless/Fresh worker,
its immutable assignment and a complete synthetic domain. Nested/specialist,
Resume/Native, unknown worktree ownership, affected additional providers and
unsettled steering responses refuse. Independent active sibling results remain
valid. The expected inventory template in the original identity is only a match
witness; it does not assert that an active actor has already settled. Readiness
requires later complete trusted fixture observations and exact source binding.

## Checkpoints, whole jobs and claims

Guards run inside actual worker result and decision acceptance, assignment
reservation, routing and join mutations, scheduling and dispatch, resource cleanup
and recovery. Already-running jobs recheck after awaits. Permanent original task
and activation fences prevent late callbacks from changing successor results,
sessions, attention or usage. A no-op task-state callback never authorizes a later
registry call. Fixture activations cannot fall back to ordinary provider signalling
or cancellation. Losing the fixture scope denies all new provider issuance from
that run, including later unrelated activations; synthetic history cannot become
production launch authority.

Readiness and release additionally require the registered **entire outer node job**
to have returned. The completion callback checks the exact task object, dispatcher,
job and source before persisting its acknowledgment. A returned `_dispatch`, task
result or caller-provided `job_quiescent` boolean is insufficient. Successor issuance
captures its own outer job; advancement archives the prior job/journals and cannot
reuse that old completion proof for a later control request.
Positive complete settlement publishes an explicit certificate-bound logical
cancellation disposition for the old workflow activation/task. It never fabricates
a worker result or exit code and never rewrites the provider's `TaskRecord`.
Permanent positive settlement proofs survive later observation invalidation, so
settling and resource readers do not treat a retired original as running forever.

For the supported fixture resource topology, the existing resource ledger transfers
the same exclusive reservation atomically from the exact old attempt to a
coordinator and then the exact successor. No intermediate release or grant pass
admits a queued competitor. Transfer intent precedes each leaf transaction and its
acknowledgment follows in the workflow CAS. An uncertain handoff keeps its claims;
repair evidence refuses ambiguity and never reconstructs the retired owner.
Current and archived journals bind the actual source reservation, names, owner,
captured grant generation and deterministic coordinator/successor identities;
valid-shaped references to another reservation are not transfer authority.
Borrowed, lent and descendant resource topologies are explicitly unsupported.

Generic cleanup, owner resolution and repair respect these controlled reservations.
The coordinator exclusion is separate from a live old execution grant and survives
until compatible successor claims are held. Actual OS worktree/effect enforcement
remains outside this synthetic profile.

## Bounds and adoption

The existing request/certificate, inventory, read, metadata and text limits remain.
Workflow writer preflight accounts for the embedded component and normal steering
together, including completion reservation and exact replay. There is no inherited
prestart 32/64 expiry for issued-effect evidence or lifetime conversation-count cap.

This is an internal conformance implementation, not a supported ctl/MCP endpoint.
The supported fixture bundle, immutable candidate bundle and product capabilities
remain distinct. Real adapter/profile qualification, production authority and
runtime selection, Rust adoption, supported publication, consumer integration and
release remain separate gates.
