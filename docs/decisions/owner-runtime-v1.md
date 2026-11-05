# Owner-runtime profile v1

2026-11-05. Owner policy approved; independently reviewed design, implementation pending.
Canonical specification assets for the Rust migration, not an implemented Python
runtime or an advertised public capability. HarflowMCP remains the contract owner
until the explicit ownership transfer. Release freeze remains in force.

## Approved decisions and implementation ownership

The owner approved these four choices:

1. Retain reservations and require attention when ownership, capture, containment
   or reconciliation is unresolved. No automatic adoption, replacement, replay or
   release follows from missing acknowledgment, a saved PID or a terminal record.
2. Preserve agent-result outcome separately from background obligations and safe
   settlement. Unknown obligations are required. Failed results remain failed;
   incidental background work cannot rewrite a valid result into failure.
3. Require measured OS enforcement for the strict permission mode. Unsupported or
   unknown combinations refuse. Tool-level permission policy is a separately
   named, explicit opt-in and must disclose its weaker threat boundary.
4. Isolate persistence in one process per operation so storage data I/O does not
   force the owner coordination loop to wait on that syscall. Retain a dedicated
   owner rather than a mandatory central service. No universal bounded process
   termination or shutdown guarantee is made.

Runtime implementation belongs in Rust. Python steering is a bridge to unblock
the Mac application; accepted steering contracts are also part of Rust's eventual
destination. This specification does not require duplicate Python owner/storage
machinery or wait for completion of unrelated Python steering work.

## Specification component and support boundary

The independent component is `contracts/candidates/owner-runtime-v1/`, with its own
manifest and digest. Its metadata must say `specified_unimplemented`,
`synthetic_specification` and `python_runtime_support: false`. Its examples are
normative model specimens, not captured ctl/MCP responses or measured OS evidence.

Documents use `format: harflow.owner-runtime.profile`, integer `v: 1` and a closed
`kind`. They are not `harflow.ctl` envelopes. Supported `contracts/v1`, its aggregate
manifest, MCP tool schemas, product version, Python request handling and advertised
capabilities are unchanged. A future capability is reserved in this design only;
it cannot appear in an available-capabilities specimen until the accepting runtime,
refusals and consumer behavior are actually qualified.

Rust pins this component by exact source commit, manifest digest and per-file
hashes. Its existing supported 0.0.10/full207 input remains separately pinned.
Importing this component does not adopt a newer aggregate steering bundle or
authorize automatic Mac/runtime selection.

The standalone generator/checker owns only this candidate directory. It uses the
standard library, imports no engine/server/backend, accesses no state root and
executes no provider. Checking requires deterministic bytes, exact inventory,
bounded documents and explicit accept/refuse/unsupported expectations, including
negative specimens. A fixture's claimed evidence is never runtime authority.

### Candidate document grammar

The exact required fields, types, closed enums, precedence and decision outputs
are normative in [the schema companion](owner-runtime-v1-schema.md).

Every specimen has `format`, `v`, `kind`, `binding` and `data`. The binding has
lowercase32-hex operation/session/reservation/attempt/root-generation identifiers,
owner PID/start/boot, root device/inode/protocol identity,64-hex request/configuration
digests, and optional role/checkpoint/revision context. Missing required identity,
booleans used as numeric identities, malformed digests or inconsistent contexts
refuse. Optional unknown fields are ignored only within the document/field budgets;
unknown kinds, versions or effect-bearing enum values are unsupported, not defaults.
No unknown field can supply an otherwise missing required proof.

| Closed `kind` | `data` and model decision |
| --- | --- |
| `policy` |Profile version, background requirement, requested permission mode and idle duration. Validate finite positive duration≤21600, reject booleans, preserve the600s default only for a valid explicitly selected policy. |
| `permission` |Requested/established mode, qualification state and exact binary/configuration/grant match observations. Admit only the requested, established, qualified mode; tool-level requires explicit opt-in. These are hypothetical trusted-adapter observations, never caller-submitted proof. |
| `observation` |Result state, sticky prior failure/later unresolved turn, requirement, obligation lifecycle/completeness and independent exit/EOF/input/capture/watermark/containment/writer/claim facts. Return separate display result, workflow settlement and release disposition. |
| `storage` |Retained pending command identity and observed acknowledgment identity/state, sequence, revisions and payload bounds. Match every field before advancement; unknown commit, mismatched or late replies retain attention and cannot reopen admission. |
| `steering_guard` |An attempted use of a result, acknowledgment or observer attachment as withdrawal/retirement/replacement permission. Always refuse that substitution; this profile supplies no steering-control proof. |

Fixture expectations distinguish structural validity/unsupported versions from
model outcomes such as retained attention. A valid observation can correctly
produce blocked settlement. Required failure is preserved; unknown inventory is
not empty. The manifest records exact expected classifications and reasons, not
merely whether a JSON parser accepted the file.

Each document is at most24576 bytes, nesting at most16, strings at most4096 bytes,
and arrays at most64 entries. Use explicit malformed/future/missing/unknown and
forbidden-action cases. Raw storage examples use bounded sample bytes and their
declared lengths/digests; no encoded full-size prompt or raw stream is required.
Runtime binary frame/payload boundary tests remain separate actual codec evidence.

## Immutable policy and binding

An explicitly selected profile request records:

| Field | Meaning |
| --- | --- |
| `profile_version` | Exactly1; unsupported future versions refuse before effects. |
| `background_requirement` | `required` or `incidental`, declared by the qualified caller or saved workflow contract. Missing/unknown remains required for settlement. |
| `permission_mode` | `os_enforced` by default for this profile, or explicit `tool_level`. No fallback. |
| `background_idle_seconds` | Resolve once, persist and inherit; preserve the existing finite positive range through21600s and600s default. |
| Other lifetime budgets | Execution, input, continuation and observation have distinct purposes. An observer deadline never stops execution or resets another budget. |
| Resource envelope | Explicit queue, raw/event, read-work, retention and admission limits with exhaustion dispositions; qualification limits are not production defaults. |

The binding carries root physical identity/protocol/generation; operation, session,
reservation and issued attempt; request and configuration digests; original owner
PID/start/boot identity; caller role and exact grant identity; and applicable
workflow/checkpoint/revision/steering tokens. IDs and environment markers are
correlation data, not authentication.

Qualify the caller through existing trusted transport and OS lineage rules before
handoff. The inherited one-use private channel transfers exact executable
preparation; a stored row or receipt cannot execute. Revalidate root, backend,
configuration and saved grants before effects. Unknown/changed authority refuses.
This adds no caller role, enrollment route or responder credential exposure.

All policy and applicable authority scopes participate in request/configuration
digests and idempotency. A child or worker cannot downgrade a required monitor or
grant inherited from its parent workflow. Model-written output cannot classify
itself incidental. Initial policy may classify the operation's background work as
one aggregate; finer per-obligation authority is not invented here.

Under the first Rust schema5 implementation, static policy is committed before
claim inside `PreparedRequest.payload` as a bounded canonical preparation envelope:
format `harflow.owner-runtime.preparation`, version1, selected profile manifest
digest, static policy, configuration digest, exact operation/session/reservation/
attempt/generation, and authority context (caller identity, role/checkpoint/revision,
issued attempt and grant digest). It contains no credential or learned-later writer
identity. Its existing payload SHA-256 is the request digest verified by Store and
bound through claim/spawn/acceptance. Cross-check duplicated outer IDs and static
context before effects; do not bind policy only in volatile IPC. The bounded opaque
configuration digest references the qualified prepared invocation, not a grant by
itself. Historical arbitrary payloads are never interpreted as this profile.

The original caller can differ from the detached owner. Do not add an owner PID
learned after preparation to the already committed static payload. The private
invitation binds that fresh owner identity, while the later participant record
and transport binding add the fresh storage identity without rewriting preparation.

Historical requests without the profile remain explicitly legacy/unrecorded.
Their existing disclosed behavior is not certified as this strict profile and is
not silently migrated or given invented proof. Existing Python status and
permission semantics remain documented as implemented.

## Independent result, obligation and settlement evidence

Do not repurpose the existing `outcome` object, which describes observed
Git/artifact/command facts. The profile defines these separate observations:

| Observation | Required semantics |
| --- | --- |
| `agent_result` | `unknown`, `succeeded` or `failed`, bound to the exact operation/attempt/turn and correlated result witness. Sticky failure and later unresolved input/turns remain visible. |
| `background_obligations` | Declared requirement, observed lifecycle, bounded items, provenance and inventory completeness. Missing/partial/unclassified evidence stays unknown, never an empty successful inventory. |
| `settlement` | Independent process exit, both-pipe EOF/drain, input reconciliation, capture, final observation watermark, containment, writer settlement and claim reconciliation. |
| Retained attention | Why current evidence blocks settlement or release; no implicit control action or deadline-driven replacement. |

A result can be displayed while its owner still runs. It is not process exit,
task settlement, resource release, or permission to resume a live session.
Required/unknown obligations that are pending or unknown block workflow settlement.
A positively observed failed required obligation settles as failure only when the
remaining execution/drain/reconciliation facts permit settlement; it is not kept
pending merely because it failed.

Incidental background failure does not change the separately witnessed agent
result. It cannot conceal an execution failure, cancellation, unresolved later
turn, stale result, capture gap or unknown failure cause. Existing terminal status
is not renamed into a result field. Observed terminal publication still requires
the original owner's exit/drain/input boundary; notifications and continuation do
not arise merely from a result message.

Workflow use of a result additionally requires the exact current issued-attempt
and final-watermark gates. Required/unknown obligations and incomplete execution
or observation block it. Resource/session release independently requires accepted
containment, writer and claim reconciliation. A successful result or terminal row
alone is never release proof. Display, workflow settlement and resource release
must not be collapsed into one boolean.

Inventory overflow retains bounded evidence, marks completeness partial/unknown
and blocks dependent actions. Counts unavailable from the source are null/unknown,
not zero. Historical observed results remain historical; they are not recomputed
from today's repository or permission settings.

## Permission admission

`os_enforced` requires a qualified effective filesystem boundary covering the
exact requested writable roots and protected-target exclusions. Other guarantees,
including network access and publication restrictions, remain separately measured
claims. A capability flag or requested sandbox pin is not proof of effective
confinement under inherited configuration.

`tool_level` requires explicit opt-in and qualified native tool-policy evidence.
It reports requested mode, established mode, exact qualification identity and
limits. It is never described as OS confinement, and it does not add commit/push,
control or writable-path authority beyond the exact existing grant.

Evidence is produced by a trusted qualification adapter, bound to binary digest,
version, platform, configuration/probe revision and exact grants. Caller-supplied
JSON saying `passed` is not such evidence. Unknown, unsupported, stale or changed
evidence refuses before effects. Revalidate at launch/resume/continuation and any
input requiring new authority. Never fall back to tool-level or unrestricted.

The first Rust implementation may qualify only fixed unpaid helpers and refuse
every stock-backend strict request. Synthetic fixtures establish model behavior,
not an effective OS sandbox or a newly supported backend/version.

## Isolated persistence architecture

| Participant | Exclusive responsibilities |
| --- | --- |
| Original owner | Root-only hold without a SQLite connection after readiness; original agent/stdin/sole waiter; reader; original storage-child waiter; one pending command and last acknowledged projection. |
| Reader | Only moved agent stdout/stderr, finite data FIFO, sticky discard and immutable final observations. No storage or mutation authority. |
| Storage process | Independently acquired Store/root lease, raw files/sentinels, narrow fenced executor and one admitted mutation. No agent pipes, stdin or signals. |
| Observer | Qualified read-only health/evidence; no waiter, stop/discard, input, grant or mutation authority. |

Bootstrap filesystem/identity qualification can block before readiness. Keeping an
unused Store in the owner is insufficient because Store retains a SQLite
connection whose operations/close can block. Both processes independently hold
the root protocol lease; the writer retains its claim through raw-file and SQLite
close. Owner death therefore cannot expose a still-live writer to root maintenance.
Preserve the256 permanent sentinel/quarantine cap, no eviction, safe spawn and same
physical inherited-CWD restriction. No unsafe callback or descriptor-passing seam
is introduced.

Use a split-phase coordinator, not a synchronous OwnerPort RPC proxy. Prepare one
command, incrementally transmit/read bounded frames, and advance state only after
its exact acknowledgment. Every owner-loop step has finite I/O work and at most
20ms poll; a pending storage operation cannot monopolize the coordination loop.
Continue agent drainage, sole-wait observations and bounded volatile health views.

Private binary framing is versioned separately from the old fixture protocol.
Each request/ack binds root/protocol/generation, operation/attempt/owner, storage
identity and fresh channel nonce, request/configuration digests, expected fences
and revisions, sequence, command kind and exact command/payload digest. Applicable
steering tokens remain in the immutable binding. One pending mutation, no pipeline,
resend, reconnect replay or automatic storage-process restart is permitted.

The private frame has a four-byte big-endian total-body length, distinct magic and
version, closed message tag, monotone64-bit sequence,32-byte immutable binding tag,
32-byte command digest and a strictly bounded typed body. Define digest input as
the magic/version/tag/sequence/binding/typed-body bytes, excluding the length prefix and
digest field itself. Acknowledgments echo the original command digest and include
their closed disposition and validated snapshot. Parsers reject truncation,
trailing bytes, oversize lengths and noncanonical scalar encodings before effects.
Bootstrap derives the binding tag from the exact source/request/policy and both
fresh process identities plus channel nonce. No digest or nonce alone authenticates
a caller; the inherited channel and qualified original process handles do that.

The closed executor supports bootstrap, claim, child recording, raw creation,
acceptance, capture begin, single-stream append/publication, known-loss publication,
terminal capture and resource close. It accepts no arbitrary command/SQL/path,
repair, reset, release, adoption or agent-control request. Append performs raw
append, sync, path/length revalidation, fenced index/event commit, then acknowledgment.

Durable storage-participant provenance is separate from the prepared payload,
because storage identity is learned later. Persist an immutable bounded association
before executable readiness. The first Rust representation is one create-new,
bounded, immutable participant sidecar in the qualified operation namespace,
separate from SQLite schema5. It binds original owner and writer PID/start/boot,
root source stamp/generation, operation/attempt, prepared/configuration/profile
digests and channel nonce. Write and sync the complete record and its directory,
then revalidate source binding before readiness. Maximum record size4096 bytes;
no overwrite, repair or automatic deletion. It is deny-only provenance, not an
ownership grant: SQLite claim/fence transitions remain authoritative. A collision
or ambiguous write refuses executable readiness and retains uncertainty. Its
absence after a crash is unknown, never proof that a writer or effect did not exist.
Missing/partial provenance may deny dependent actions; it never grants them or
substitutes for an owner/agent identity or positive prestart proof.

## Unknown, shutdown and retention

Ambiguous command transmission, response loss, malformed/stale acknowledgment or
storage error permanently seals further mutation admission. Retain the exact
possibly committed command; continue volatile agent drain/exit facts. Late matching
acknowledgment is diagnostic evidence only and cannot unpoison the owner. Never
repair, truncate or retry an uncertain raw tail.

Healthy queue overflow preserves its accepted contiguous prefix, then publishes
known loss. Reader-discarded, acknowledged, abandoned-after-poison, uncertain
in-flight and unknown unread-tail bytes remain distinct. EOF alone, exit alone and
writer acknowledgment alone cannot produce a lossless terminal record.

Closing writer input stops future commands; it does not undo an admitted mutation.
A Close frame is permitted only while healthy with no pending command. After a
partial/uncertain TX, close input instead of interleaving another frame. Normal
teardown requires writer resource close, both original agent and storage-child
exit/reap, joined reader, reconciled FIFO and IPC completion, not merely a sent
Close command or shutdown acknowledgment. If writer
settlement is unresolved, retain the resident owner's root hold and original
writer handle in bounded attention state, without growing queues or history.
No detached live writer, unbounded join or timeout-based cleanup proof is allowed.

If the owner crashes, the writer's independent root claim remains while it may
write or close. After its admitted operation returns, private-channel loss admits
no further mutation. Neither a saved PID nor pipe EOF permits another actor to
adopt/control the writer or replay the command. Durable claims remain protected.

No universal bounded owner shutdown or process death is promised under arbitrary
kernel I/O. The supported design goal concerns storage data-I/O isolation after
readiness. Root/pipe/OS calls themselves remain nonpreemptible.

Owner-local pending command identity is retained while the owner lives. After an
owner crash, an unpersisted pending command is unknown; the participant record and
last durable capture are not a reconstructed acknowledgment. Do not replay it.

## First internal qualification envelope

These bounds deliberately preserve the current fixture line and are not production
defaults or performance acceptance:

| Resource/work | Bound and exhaustion |
| --- | --- |
| Operation/processes |One operation, one original agent and one storage process; no reuse/restart queue. |
| Threads |Owner main plus one reader; writer one application thread. |
| Reader FIFO |Four batches, at most16384 bytes per stream; sticky discard after first Full/disconnect. |
| IPC |One pending mutation, one TX and one RX frame, at most24576 bytes each; append one stream, at most16384 raw bytes. |
| Owner-loop I/O step |At most4096 bytes each of TX, response RX and writer stderr, one child-wait observation per child, and one poll≤20ms. No Store call or blocking full-frame RPC. |
| Writer diagnostic stderr |Retain at most4096 bytes, count/discard thereafter, read at most1MiB total. Exhaustion reports unavailable diagnostics/attention; it is not agent-output loss or termination proof. |
| Logical raw payload |Conservative240KiB including existing192KiB reader envelope and additional bounded transport/decoder copies; inspect implementation before reducing. Excludes metadata/kernel buffers/stacks/RSS. |
| Capture/read work |Existing65536 raw bytes per stream,128 capture events,8MiB read per stream,4096 reader rounds/8192 attempts. Limits retain incomplete/unknown tails. |
| Lifetimes |Existing owner8s/effect4s/dispatcher10s qualification budgets; separate bounded cleanup and retained-unknown handling. Observer deadlines never extend/reset execution. |
| FDs |Owner root sentinel and three storage stdio endpoints; agent outputs move, no duplication. Writer Store2+raw2 sentinels independently. No spool or sentinel-cap increase; no blanket SQLite/library FD claim. |
| Persistent participant |One immutable bounded record per operation; no automatic deletion/expiry of unresolved evidence. |

Keep the existing schema5 and fixture caps for this qualification. A root-only
hold captures the existing binding, marker/generation and shared root lease using
bounded bootstrap filesystem reads; it opens no SQLite connection. No database or
raw-file probe is required in the ready owner's coordination step. Wire hashing
may use a bounded pure digest helper; it must perform no filesystem/SQLite call.

The owner retains one reader pair until both single-stream commands have been
acknowledged; it cannot dequeue an additional pair into an unaccounted buffer.
The conservative240KiB payload accounting includes the queued/producer/consumer
envelope, encoded TX payload and both writer frame/decoded payload copies.
Bootstrap metadata plus the existing bounded prepared payload must fit24576 bytes
before transmission; oversized preparation refuses before agent spawn.

Production envelope selection must preserve existing foreground semantics and the
600s background-only default, specify each configured limit and exhaust mapping,
and be measured against R01 latency/RSS/workload criteria. Fixture constants must
not be relabeled as production budgets. No observer reconnection resets a durable
budget, and retention age never releases unresolved exclusion evidence.

## Steering and compatibility fences

An old reader origin, delayed writer reply or incidental-obligation declaration
cannot advance a replacement attempt. Withdrawn output/exit remains historical
evidence for that exact issued attempt. Result success and storage acknowledgment
are not positive prestart withdrawal, retirement, steering approval or a new grant.
Future steering effects must preserve their canonical role/checkpoint/revision,
safe-boundary and final-watermark requirements. This does not implement all ST06.

## Review, delivery order and qualification

1. Independently review this design and the exact candidate schemas/protocol.
2. Commit only the disjoint canonical component/tooling/tests locally in MCP.
3. Pin that immutable component in Rust, retaining the supported0.0.10/full207
   baseline independently; implement and test private runtime mechanics there.
4. Observe actual finite writer gates before/after sync/commit/reply/close, both
   pipes and responsive owner progress before release; malformed/partial/stale IPC,
   permanent poison, no replay, raw/index extents, root exclusion after owner death,
   writer death with a live agent, and bounded clean teardown. Use external witnesses.
5. Preserve existing tests, exact steering fences, dependency/architecture guards
   and fresh final review. Complete cohesive local commits without push/release.

Actual indefinite I/O, failed OS sync, ENOSPC, power, stock backend enforcement,
production caller/containment, cross-platform/filesystem and scale qualification
remain distinct evidence gates. A synthetic profile or successful helper does not
advertise a supported public runtime or accept full R03/R04/P2.
