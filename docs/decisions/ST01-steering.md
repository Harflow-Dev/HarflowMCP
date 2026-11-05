# ST01 — Conversational workflow steering

Date: 2026-11-05 (Asia/Jakarta). Status: proposed normative specification for primary/owner adjudication; no implementation or schema acceptance implied.
Subsequent owner/primary decision, 2026-11-05: grammar/state and authority policy A accepted
for the separately assigned ST02 builder implementation, including active-turn queueing.
The owner subsequently authorized ST03-A orchestrator implementation using ST02 evidence as
bounded entry. The owner subsequently authorized ST03-B policy B and the shared bounded-window amendment below. ST04 adapter cancellation decisions remain a later gate. Implementation
qualification, consumer adoption and owner acceptance are recorded separately in
[ST02](../migration-receipts/ST02.md); candidate later-role schemas remain proposed.

Canonical producer: HarflowMCP until accepted O04 transfer.
Baseline: `v0.0.10`, commit `8a936de61b5f9f2fdff671b65b5384259267350b`;
fixture manifest SHA256 `ceb336216b8e5b0938f101b2a8b2ba5f85e3d939e281c446461499a2ae5c4906`.
Scope and review provenance: [steering plan](../../../HARFLOW_WORKFLOW_STEERING_PLAN.md),
[session index](../../../HARFLOW_MIGRATION_SESSIONS.md), [receipt](../migration-receipts/ST01.md).
Every API/field/capability below is a candidate addition, absent from that baseline.

## 1. Settled specification choices

An explicit authorized caller request issues one bounded conversational turn at an exact target.
A successful ordinary final persists its exact text and waits for a subsequent explicit caller
request. It neither satisfies the worker result protocol nor advances any graph. A framed role
command passes the original role validator and fresh identity checks before one execution commit.
The non-steered protocol, including its existing guided JSON parser/correction behavior, is unchanged.
No caller/model conversation loop is automatic. A caller may explicitly request another turn
within the episode allowance. Discussion cannot invent configured branches, backend/model
choices, permissions, retry grants or graph edits.

Use a separate steering ledger/projection, not `needs_input` or `input_kind=steering`.
A real question still requires its actual question text and `input_decision_id`; an attention
checkpoint still requires its `attention_checkpoint_id`. Neither is the steering episode ID.
Ordinary `send_message`/builder follow-up remains context only unless the new typed operation is
explicitly used. Reusing its storage helpers must not reuse its target-following behavior.

Initial control is safe-boundary only. Admission suspends NEW affected reservations. It does not
signal a harness or erase already accepted decisions. Active siblings can finish and publish
observations; new conflicting work stays blocked. ST04 adds separately specified exact scoped
cancellation and linked replanning. Unknown spawn, exit, capture, drain or containment never
proves successful settlement or permits replacement.

## 2. Evidence that constrains implementation

These paths refer to the pinned producer above unless stated otherwise:

| Evidence | Consequence |
| --- | --- |
| `src/harflow_mcp/workflow_messages.py:43–94,111–137,161–193` | Queue is bounded; claim is atomic with dispatch reservation; uncertain claims do not replay; `now` cannot certify interruption. Ordinary task messages follow node/role, so steering needs stricter exact activation binding. |
| `src/harflow_mcp/workflow_delegation.py:25–80,971–1045,1191–1245` | Guided parser searches JSON objects; decision attempt is charged before dispatch; steering requires a separate classifier/ledger, not modifications to ordinary parsing or refunds. Role result validation remains authoritative. |
| `src/harflow_mcp/workflows.py:3430–3450,3586–3645` | Dispatch reservations and positively proven not-started versus uncertain outcomes already exist; conversation issuance needs equivalent durable intent/claim guards. |
| `src/harflow_mcp/workflows.py:4308–4350,4373–4401` | Builder feedback can automatically continue today; explicit steering must bypass that loop. Edit/follow-up yields unsaved proposals, but original create mode saves a draft definition. Do not route a steered final into that autosave path. |
| `src/harflow_mcp/workflow_interactions.py:35–91,157–182` and `server.py:2326–2343` | External parent capture is liveness evidence; responder label matching is not proof of the same original external caller. New steering needs a stronger eligibility gate. |
| `src/harflow_mcp/workflow_inputs.py:31–54,70–97`; `workflow_attention.py:34–105` | Questions route only through reciprocal nested links; attention identity has different semantics. Preserve both. |
| `docs/contracts.md`, common envelope, handshakes, caller-loss sections | Fresh per-operation capability/root/line checks; unknown event kinds optional; existing shared answering does not confer new steering authority. |

Mac and Rust source observations and their exact heads are in the receipt. Native checks are
source inspection, not decoder/UI qualification. No current backend is newly qualified here.

## 3. Caller eligibility and interaction ownership

### 3.1 Trust boundary

The local engine currently observes process lineage and process generation, not authenticated
remote users or a cryptographically authenticated Claude Desktop identity. Do not describe this
as secure authentication against hostile programs with the same OS account. A tool name,
`interaction_owner="caller"`, supplied PID, run ID, session label or MCP connection ID is not
identity evidence. The new operations accept no caller-provided identity or role assertion.

Derive a `principal_binding_id` internally from qualified engine admission provenance; public
IDs are opaque references, never bearer authorization. Store original external process identity
and root relationship before detach. For each mutation, positively determine managed versus
external lineage and independently recapture the external ancestor using bounded revalidated
links. External caller is eligible only if that identity equals the admitted root external parent,
is presently qualified, and its binding epoch is current. Shell/script/unknown ancestry refuses.
A managed workflow worker, builder or orchestrator is refused before target lookup, including
attempts through ctl/`--monitor`; managed access cannot create an external ownership binding.
An external caller that launched a workflow through a managed intermediary is not retroactively
inferred to own it. Managed parent control continues only through its existing scoped protocol.

Native mutation eligibility uses the existing positively verified human ctl gate, never the
flag alone. This is the baseline local human distinction, not application signing attestation.
No new permission-expansion, approval-response or TUI takeover endpoint is added over MCP.
Approval request IDs/digests and responder credentials remain completely separate.

### 3.2 Proposed ownership policy (material owner decision A)

Keep the original external caller binding and native human eligibility as two eligible responders
on a caller-owned root, following existing shared-interaction policy. Admission/reply requests
use a root-wide `interaction_epoch` CAS and the episode revision CAS: the first valid request
wins. A losing caller gets the winner's public receipt, with text only if independently readable.
An accepted request does not transfer root ownership. A second responder must refresh before
issuing a new turn; no concurrent turn for one episode. Multiple independent episodes in one root
are serialized for issuance in the initial version; active sibling execution remains concurrent.

An explicit native-human `take_owner` action changes root steering eligibility to monitor-only;
`release_owner` may restore the STILL-qualified original external binding. Both increment epoch,
record old/new binding and reason, invalidate all in-flight steering commands, and retain any
running harness until observed settlement. Neither resumes scheduling. This is interaction
ownership only, not existing terminal/TUI takeover, and never signals a process. MCP cannot
self-grant takeover or delegate ownership. Existing ordinary interaction ownership APIs remain
unchanged; their mutations must invalidate the steering epoch if they change effective authority.

Server/transport reconnect while the same external process generation remains alive can rebind
a new stdio server to the same admission identity after full verification. Response loss is handled
by request receipt lookup. External application restart (new PID/start generation), missing old
provenance, changed root, or unknown ancestry does not reconnect by name. Recommended initial
policy: native human explicitly adopts steering; granting a replacement external application a
new binding is deferred until a separately accepted enrollment/authentication design. No reusable
secret/token is introduced by ST01. This limits caller recovery rather than pretending existing
process evidence survives application restart. Workflow execution history is unaffected.

## 4. Durable records, revisions and transitions

Candidate root record adds `interaction_epoch` (nonnegative integer) and a qualified
`steering` container with `schema_version:1`, `control_revision`, `episodes`, `requests`,
`turns`, `commits` and monotonically increasing `sequence`. Historical roots lacking provenance
refuse steering; legitimate engine admission/migration can add qualified data, never fabricated
owners. Persisted schema qualification/migration is a producer change in ST02.

A target snapshot contains ALL applicable values, explicitly null when inapplicable:

```json
{"root_workflow_run_id":"root1","workflow_run_id":"run1","path":["root1","run1"],
 "role":"orchestrator","node_id":"start","token_id":"tok1","activation_id":null,
 "task_id":null,"decision_id":"dec1","checkpoint_revision":4,"goal_revision":2,
 "definition_digest":"sha256:issued","assignment_revision":null,"assignment_digest":null,
 "draft_revision":null,"source_revision":null,"source_workflow_id":null,
 "source_definition_digest":null,"preview_hash":null,
 "interaction_epoch":3,"control_revision":7}
```

`steering_revision` starts at 1 on open and increments on each accepted explicit reply, retarget,
dismiss or eligibility-invalidating action. Reservation/discussing/reply-persistence observations
advance sequence, not that issued command revision. Admission validates the supplied PRE-admission
snapshot, then creates the issuance/commit witness from the POST-admission checkpoint/control
revisions and returns it to the harness/caller. State-only observation never self-stales a turn.
Own preview updates advance commit witness/checkpoint/draft revisions while retaining that
turn's issued steering revision. External semantic changes invalidate it instead.

`checkpoint_revision` is NEW per-target semantic state, not existing run `revision` (definition
revision), timestamps or attention token. Changes to valid continuations, accepted results,
question/approval state, assignments, goals, draft/preview, selected branches, source ownership
or effective grants increment it. Log tail/activity within an unchanged attempt does not.
`control_revision` changes on steering admission/ownership/control changes. All compared values
come from an engine-issued observation; supplied IDs alone never establish a source link.
Root and every nested parent/invocation edge must be reciprocal, bounded, acyclic and unchanged.
A worker includes exact activation/task, immutable issued assignment digest/revision and token;
a builder includes its run, draft revision, preview hash and saved-source revision/digest.

| Durable state | Allowed transition / effect |
| --- | --- |
| absent | Authorized `open` CAS persists episode/request/text/hold; becomes `queued_boundary` or `ready`. |
| queued_boundary | Existing issued work settles normally; same target still eligible becomes ready. Changed target becomes `stale`; unknown stays `delivery_unknown`/held. Never select a next node automatically. |
| ready | One authorized request plus capacity reserves one turn and claim, becomes `turn_reserved`. Opening request supplies the first turn permission. |
| turn_reserved | Proven no-start can remain ready with the SAME issuance/request identity; observed launch becomes `discussing`; ambiguous launch becomes `delivery_unknown`. |
| discussing | Persist observed terminal evidence and exact final before classification; ordinary successful final becomes `awaiting_caller`; valid command becomes `committed`; invalid attempted command becomes `command_refused`. |
| awaiting_caller / command_refused | Explicit next `reply` with current revisions reserves one turn; otherwise no dispatch. No automatic correction prompt in steering mode. |
| stale | Retain text/reply and hold; explicit `retarget` after a fresh observation opens a successor episode linked to this one. No silent fallback to ordinary messaging. |
| delivery_unknown | Observe/reconcile SAME task/claim only; no resend, replacement or release without positive evidence. |
| committed | Terminal episode receipt; normal accepted role transition owns subsequent execution. Exact request/commit replay returns receipt without effects. |
| dismissed / expired / target_terminal | Terminal episode, preserved history. Dismissal clears a settled steering hold but never schedules work; expiry retains the hold for explicit dismissal. Neither releases uncertain claims. |

`budget_exhausted` is an awaiting-caller reason, not successful execution. Provider failure,
timeout, cancellation, usage limit, empty final and capture loss have distinct turn outcomes.
A structurally completed builder run can be targeted for a new draft conversation; a terminal
non-builder run cannot be reopened through steering. Actual pending question/approval or unknown
control blocks turn issuance with an explicit reason; steering cannot answer it implicitly.
Dismiss while a turn is running invalidates its command and requests no process interruption;
its hold becomes `dismiss_pending_settlement` until positive settlement, then clears without
scheduling. For a settled episode, dismiss atomically clears that episode hold and increments
control revision while leaving the workflow paused. The ordinary pause checkpoint is legitimately
persisted at that mutation; subsequent ordinary resume/recover uses that real checkpoint. A new
open racing resume shares the root lock: the winner changes control/checkpoint identity, and the
loser refuses stale. Stale/expired holds require explicit dismiss or retarget; no timer lifts them.
Any unknown attempt/resource claim remains independently blocking after steering hold removal.

### 4.1 Safe-boundary linearization

Acquire the root tree mutation lock and revalidate source and authority. This is the serialization
point shared by steering admission, decision acceptance, execution reservation and control changes.
No process wait, provider call, Git command or resource wait occurs under it. Implementation
must preserve existing leaf-lock ordering. Multi-file root/child delivery uses a durable root
intent plus an idempotent child receipt and final root acknowledgement, not an assumed multi-file
atomic write. Any incomplete handoff keeps the root hold effective; recovery completes only the
same exact intent or reports unavailable, never repeats effects.

If admission wins before decision acceptance, increment checkpoint/control revision and prevent
the older response committing. If the older decision wins, admission refuses stale against the
observed snapshot; it cannot retract the accepted route. If a harness turn was reserved/spawned
before admission but its command is not accepted, queue until its actual outcome settles; retain
its output as superseded and forbid its old command. No second harness uses the same live session.
Execution already reserved by an accepted decision is real issued work; ST02/ST03 do not withdraw
it. ST04 needs a separate proven-not-started withdrawal or settled linked-replan action.

The hold is scoped to the target activation/decision and its successor scheduling dependency;
initially block new root orchestration decisions too, so root checklist/route state cannot race
steering. Unrelated active siblings may publish terminal evidence and already-issued child owners
may settle their issued work. Prevent NEW reservations in the affected subtree; do not claim a
root hold instantly stops an already-issued child's internal owner. If no qualified shared hold
exists at that seam, refuse the operation as unsupported rather than falsely reporting suspension.
Sibling completion changing issued choices increments checkpoint revision: persist the discussion,
mark it stale, and require explicit retarget; do not discard completed branches or their artifacts.

## 5. Exact final-output grammar

A steering prompt gives the harness the issued turn/episode identity and this portable framing.
It works with final text alone; no engine tool call is required. Only the harness's verified final
assistant channel is classified; tool output, transcript quotes and streamed partial text cannot
commit. Successful observed exit and qualified complete final capture/drain are prerequisites.

A command is exactly this complete document (optional leading/trailing ASCII whitespace):

```text
HARFLOW_COMMIT_V1
{"episode_id":"ep1","turn_id":"turn1","commit_target_ref":"commit-target:issued",
 "steering_revision":1,
 "interaction_epoch":3,"checkpoint_revision":4,"goal_revision":2,
 "role":"orchestrator","command":{"decision_id":"dec1","action":"continue",
 "reason":"Use the configured review route","next":[{"continuation_id":"issued-review-route",
 "prompt":"Review the implementation against the accepted plan","session_mode":"fresh"}]}}
END_HARFLOW_COMMIT_V1
```

The example is shape-valid when `issued-review-route` is an actual issued agent continuation;
placeholder identities never authorize execution. Actual bundles must contain issued choices. Wrapper members are a closed set; identity values
must match the durable issued turn and its current engine-issued `commit_target_ref`. Each turn
starts with a commit witness equal to its full immutable issuance snapshot. Wrapper
`checkpoint_revision` and builder command draft revision follow the CURRENT witness; original
issuance revisions remain separately retained. Only the builder own-preview rule in section 6
can advance that witness without a new explicit caller turn; all other target changes stale it.
The reference is not a bearer credential and must resolve to that exact turn/current epoch. Worker uses
`role:"worker"`, `command:{status,result,evidence}`; builder uses `role:"builder"`,
`command:{definition:OBJECT,expected_draft_revision:INTEGER}`. Builder command yields an unsaved
proposal only. Original role validators, context acknowledgements, completion evidence, valid
choices, branch bundles, limits and source guards still apply. No field for arbitrary backend,
model, permission, graph override or execution-tool invocation is added. A valid nonterminal role
action such as `inspect` or `call_on_demand` commits that normal action once and closes the episode;
its subsequent behavior follows existing normal protocol/budgets, not a conversational loop.

Recognize command intent only by either (a) reserved framing tokens on their own top-level lines,
or (b) a whole-document JSON object / whole-document JSON code fence with role-shaped command
keys. For (b), refuse `commit_frame_required`; never execute an unframed legacy command and never
downgrade it to prose. Top-level means outside Markdown code fences, blockquotes and inline-code
spans. Whole-document role-shaped JSON includes any of `decision_id`, `action`, `status` with
`result`/`evidence`, `definition`, or steering wrapper keys. A whole-document JSON-like value
starting `{`/`[` with incomplete syntax is `malformed_command` rather than conversation. This
conservative whole-document rule requires quoting/explaining JSON examples. A whole-document
unrelated valid JSON object is conversation, not a result.

The parser must consume the entire framed body as strict UTF-8 JSON: one object, no duplicate keys,
nonfinite numbers, comments, trailing objects or trailing prose. One frame only, case-sensitive
ASCII delimiters, LF or CRLF line endings; preserve raw bytes in history. A stray end token,
truncated start token line, frame inside surplus prose, multiple frames, missing end, fenced frame
as the entire document, or conflicting command objects is refusal. A framing-looking line beginning
`HARFLOW_COMMIT` outside quoted/example context is malformed reserved intent, not a typo to ignore.
A quoted/fenced frame embedded in explanatory prose remains conversation. Markdown parsing uses a
pinned lexical subset below with adversarial fixtures; unsupported/ambiguous quoting refuses
classification instead of speculative execution. The subset is normative for steering intent,
not a claim to implement all Markdown rendering:

- A line with 0–3 ASCII leading spaces followed by at least 3 equal backticks or tildes opens
  a fence; its closing line has the same character, at least that length, then spaces only.
  Backtick opening info cannot contain backticks. Fence body is quoted example context.
- Outside a fence, a line with 0–3 spaces then `>` is a blockquote line; only that line is
  exempt (no lazy continuation). A line beginning a tab or at least 4 spaces is code context.
- Inline backtick spans close with the same backtick-run length on the same line. Delimiters
  inside a valid span are quoted. Unclosed spans/fences with reserved intent or command-shaped
  content yield `ambiguous_output`; no command execution or conversational success.
- Exact command documents may have only blank ASCII-whitespace lines around the frame. Frame
  delimiter lines themselves have no indentation. Indented reserved intent outside code/quote
  context is malformed, never executed. Whole-document code-fenced or indented commands are
  `commit_frame_required`; explanatory prose is required to make such a document an example.
- Nested blockquotes/fences follow these lexical exclusions only; escaped delimiter-like text
  is ordinary prose unless the whole-document JSON rule applies. Goldens freeze tilde/backtick
  lengths, 0–3 spaces, tab/4-space code, CRLF and unclosed-context outcomes. No substring search for decision IDs.

| Final example / condition | Classification / effects |
| --- | --- |
| `Let's divide implementation and review among the configured agents.` | Conversation; exact reply persisted, wait; zero graph changes. |
| `The old decision_id was dec1. I would change the division.` | Conversation, regardless of ID existence. |
| Prose followed by a fenced JSON example / `> HARFLOW_COMMIT_V1` example | Conversation, no command intent. |
| Whole document `{"decision_id":"dec1","action":"continue"}` | `commit_frame_required`, not prose. |
| Whole document `{"decision_id":` or `[...` | `malformed_command`; never conversation success. |
| Exact framed valid current role command | Commit once under fresh authority/target CAS. |
| Exact frame with missing ID, wrong role, stale revision, duplicate JSON key or invalid branch | Typed command refusal; no automatic corrective turn. |
| Two frames, frame plus prose, bare end marker, truncated reserved prefix | `malformed_command`/`conflicting_command`; no effects. |
| Whole-document code fence containing frame | `commit_frame_required`; examples need explanatory prose. |
| Empty/whitespace final; exit failure despite good-looking frame; partial capture | `empty_output`, `provider_failure` or `capture_unknown`, never conversation/commit. |
| Timeout/cancel/usage limit with otherwise plausible prose | Separate observed failure category; no successful conversation or commit. |
| Same prose with no steering grant | Existing strict/guided protocol and ordinary failure/correction behavior. |

## 6. Accounting and side effects

The owner-approved ST03-B amendment removes lifetime steering-count quotas for builder,
orchestrator and worker conversations. Each explicit caller message authorizes one finite
provider response. It never consumes, refunds, replenishes or resets ordinary execution turns,
node attempts or decision attempts, and never authorizes an automatic conversation or repair loop.
The 32 settled-turn and 64 settled-receipt windows are soft retention targets, not admission
quotas; byte pressure can retire them earlier. Retained conversation text is bounded to 1 MiB
and inline metadata to 4 MiB across representations. Caller text remains at most 32 KiB and
final text at most 64 KiB. Each provider response has a separate finite harness cap of at most
8, honoring a smaller configured positive cap; episode age can require explicit retargeting.
Unknown/in-flight reservations, current managed session heads and unresolved effect claims
are never evicted. Backpressure occurs when protected evidence cannot fit, never at a cumulative
count. Settled effects retire only after their authoritative normal result/decision proof is durable.

Storage epochs are separate from ownership/interaction epochs. Close the old request namespace
atomically before pruning its receipts. Retained exact requests replay their original receipt;
a missing retired-epoch request expires and cannot spawn or grant authority. The same UUID in
a new epoch is an explicitly NEW scoped request, not replay of an old one. History uses absolute
sequences and explicit gaps; lifetime raw transcript or exact replay is not promised. Permanent
issuance accounting and observed usage survive compaction; missing cost/usage stays unknown.
Task-bearing evidence can retire only after durable managed ownership and current session-head
proof prevent surviving raw tasks from becoming independently resumable. Qualified v1 migration
requires explicit schema 2 upgrade by the original qualified caller or verified native human;
no automatic state import, ownership transfer or consumer adoption is implied.

Reserve a separate `conversation_issuance` before dispatch, with immutable request ID, turn ID,
provider task ID, separate resource reservation and candidate binding. No generic hard dollar cap is claimed. It never increments
then refunds `decision_attempts` or a worker graph attempt. Issuance count is charged permanently
even for failed/no-start/unknown turns. An unresolved delivery cannot be replaced by a new request.
Actual provider token/cost/harness-turn usage is recorded independently
with observed/estimated/unavailable provenance and ordinary run statistics. Unknown spend keeps
the reserved upper bound; never report zero. No automatic repair turn or fallback after uncertain
launch. A positively never-started response settles its issuance; another response requires a new
explicit caller message under the configured candidate policy.

A valid orchestrator command consumes one normal decision acceptance allowance atomically at
commit (also require that allowance remains). A refused steering command consumes its conversation
issuance/refusal count, not an invisible execution retry. Worker final commits the result for the
existing node activation; it does not create another attempt. Builder proposal uses its existing
draft CAS. Budgets for inspect/on-demand actions remain independently effective.

Worker policy B is owner-approved for ST03-B: every explicit authorized message grants one
finite separately accounted response, without a fixed lifetime node/activation/root count or
extra `worker_tool_turn` quota. Existing configured resources and permissions remain effective;
actual issuance and observed usage persist, and unknown reservations are never treated as zero.
Unchanged
backend/network/filesystem/tool permissions apply to all role conversations unless independently
qualified restriction exists. No enforceable per-shell-side-effect count is claimed; reject a
policy demanding such a count on an adapter without it. Tool calls may write files, spend money
or cause external effects even when the final is only prose. Output commit validation cannot undo
those effects. Builder remains configured read-only and cannot dispatch agents; do not overstate
that harness mode as OS confinement. Role restrictions still apply during discussion.

Builder preview publication is distinct from final proposal commitment: existing active-builder
`apply_workflow_draft` remains usable with its draft CAS. In a steered turn, record each accepted
own preview mutation as a turn-owned receipt. Preserve the immutable issuance snapshot and
advance a separate `commit_target` witness only for those exact own preview writes, returning
the updated witness/revisions in the tool response. The final frame must match that latest
witness and expected draft revision. Any external canvas/source change invalidates the witness
and requires explicit retarget. A prose final after preview publication preserves that preview
and reply but does not fabricate a final proposal, Apply, Save or run completion. For tool-free
harnesses the final framed definition uses the unchanged issuance draft revision. This explicit
producer adaptation is required; ordinary preview publication must not silently self-stale a turn.

All enabled adapters must expose a qualified finite issuance control (harness-turn bound and/or
provider spend cap as actually enforceable). An observed cost estimate is not a hard cap. If no
finite requested bound can be enforced, return `steering_budget_unsupported` before provider
launch; do not pass unsupported `max_turns` flags or claim a prompt limits tools. Resume must
verify candidate/session/root/role/assignment bindings and liveness; a definitely unavailable
session may use a disclosed fresh-context path only if caller requested `allow_fresh_context:true`.
Uncertain session/launch never uses that path. No runtime/backend name special cases outside
adapter seams. Backend qualification requires later assigned deterministic evidence, not probes here.

## 7. Proposed public ctl and MCP operations

Keep `harflow.ctl` v1 envelopes and independent capability gates. Add capabilities incrementally:
`workflow-steering-read.v1`, `workflow-steering-builder.v1`,
`workflow-steering-orchestrator.v1`, `workflow-steering-worker.v1`,
`workflow-steering-window.v1`; `workflow-steering-replan.v1` remains a later candidate. An engine advertises a role only once implemented/qualified;
version-info is static supported functionality, not per-run caller authorization. Observations
return current eligible actions with reasons, never a guarantee a later mutation will pass.

Candidate ctl commands:

```sh
harflow-ctl workflow-steering-status ROOT --json
harflow-ctl workflow-steering-history ROOT --cursor CURSOR --limit 50 --json
harflow-ctl workflow-steer ROOT --request-file request.json --json
```

`request-file` reads bounded UTF-8 JSON as data (stdin `-` supported); no generic execute-command.
Native adds existing `--monitor` channel selection, which still needs positive human verification.
MCP tools `get_workflow_steering(workflow_run_id)`,
`get_workflow_steering_history(workflow_run_id,cursor=null,limit=50)`,
`steer_workflow(workflow_run_id,request)` map to the SAME application service/guards/error codes.
The MCP mutation never accepts `monitor`, caller binding or role authority as overrides.

Common mutation input is a closed object: `schema_version:1`, `request_id` (caller UUID),
`action` (`open|reply|retarget|dismiss|take_owner|release_owner`), `expected_interaction_epoch`,
`expected_control_revision`, `expected_steering_revision` (null only on open), `episode_id`
(null only on open), `target` (the complete issued snapshot), `text` (open/reply/retarget only),
`allow_fresh_context` (boolean, default false). Unknown fields refuse. Ownership actions need
no prose but require nonempty `reason` instead; they are native-human only. Retarget creates a
new episode and includes `supersedes_episode_id`; its text must be explicitly supplied again,
not copied or sent automatically. Dismiss may include `reason`, never a command/answer.
Ownership actions are a separate closed variant: only schema_version, request_id, action,
expected_interaction_epoch, expected_control_revision and nonempty reason; no target, episode or
steering revision is required or accepted. Thus root adoption works before any episode exists.
Every endpoint scopes its root parameter against the target before lookup disclosure.

```json
{"schema_version":1,"request_id":"06a29931-34d9-40ce-a2a2-b0324ba9755c",
 "action":"open","episode_id":null,"expected_steering_revision":null,
 "expected_interaction_epoch":3,"expected_control_revision":7,
 "target":{"issued_snapshot_ref":"steering-target:opaque"},
 "text":"Can we divide this among the configured Codex, Claude and Vibe branches?",
 "allow_fresh_context":false}
```

For compact transport the `target` above may be the alternative opaque `issued_snapshot_ref`
ONLY if the engine can resolve the immutable full snapshot under the same root and freshness
checks. No partial-field wildcard matching. Full-form target is shown in section 4. Snapshot
references are engine-issued, bounded, expire after 30 minutes, are not secrets and convey no
permissions. Missing/expired reference refuses and asks for a fresh observation.

Successful ctl payload example (common envelope fields included):

```json
{"product":"harflow","format":"harflow.ctl","v":1,"product_version":"0.0.10",
 "result":{"request_id":"06a29931-34d9-40ce-a2a2-b0324ba9755c",
 "payload_digest":"sha256:canonical-request","episode_id":"ep1",
 "steering_revision":1,"interaction_epoch":3,"control_revision":8,
 "sequence":12,"state":"queued_boundary","turn_id":null,
 "accepted":true,"scheduled":false,"replayed":false,
 "next_action":"Observe the same episode; no safe interruption is supported"}}
```

Version is illustrative envelope syntax, not a claim 0.0.10 supports steering. Once turn issuance
is durable, result reports its turn/task identity and `scheduled:true` meaning reservation,
not observed spawn. A read returns `steering_version:1`, root/owner epoch, exact issued targets,
episodes with state/reason/latest sequence/target/revisions, pending hold scopes, live/settling
attempts, budgets with provenance, eligible actions/refusals, latest reply reference and receipt.
No fabricated question text. History uses immutable sequence ordering with cursor bound to
root/generation/filter; 1–100 entries/page, default 50, 256 KiB page cap, explicit truncation/gap
and next cursor. Full text uses bounded continuation pages; command bodies must be read completely
before presentation as actionable. Read cursors cannot authorize mutation.

### 7.1 Idempotency, ordering and errors

Canonical request digest is SHA256 of strict JSON serialized with sorted object keys, ASCII
escaping, compact separators, no duplicate keys/nonfinite numbers, all defaults materialized;
include action/root/submitted target representation/text/expected revisions/fresh-context choice,
exclude request ID, transport and identity labels. Persist a separate resolved_snapshot_digest
alongside the exact submitted request and the immutable snapshot reference binding. An existing
request compares the submitted canonical digest WITHOUT re-resolving an expired snapshot ref;
only a new request needs fresh resolution. Full-target and ref forms are distinct payloads, so
changing representation under the same request ID conflicts even if they once resolved equally. No Unicode normalization. Request key is
`(root,principal_binding_id,interaction_epoch,request_id)`. Verify current principal before
receipt disclosure. Check an existing receipt before comparing revisions: same digest returns
that receipt, different digest gives `request_payload_conflict`. Once ownership revoked, a retry
cannot act using the old epoch; independently authorized history may show its receipt. Persist
request receipt/hold/text/sequence in the same admission transaction. Expired replay tombstones
return `request_expired` and never treat old IDs as new requests. Retain per-root bounded tombstone
index with 1024 mutation keys/root; retain compact key/digest/outcome tombstones for the root
lifetime, including expired requests. On capacity refuse new requests, never evict keys and
re-admit old IDs. Full text may expire; replay then returns request_expired without execution.
No replay across state roots.

Concurrent fresh requests at identical expected revision: one wins, others receive
`steering_revision_stale` (or `steering_busy` if a turn is active). Messages ordered by durable
sequence, not caller timestamps. Each explicit request issues at most one turn and claims only
its own text plus an explicitly enumerated context batch. Ordinary queued context may accompany
it using existing atomic message claims, but never gains steering meaning. Persist exact claimed
IDs and digest before launch. Positive not-started can restore ordinary context claims under
existing rules; uncertain claims remain delivering. Reply history is persisted before notification;
notification/HTTP/MCP loss triggers reads, never another turn. Provider delivery cannot promise
exactly once under unknown outcome; graph acceptance can be at most once under durable commit ID. Commit linearization under
the root lock revalidates the entire target/epoch, role command and current allowed choices,
charges the normal acceptance allowance, marks the turn/episode committed and persists the
accepted transition plus successor reservation intents in one durable run mutation. Validation
outside the lock is advisory. Subsequent spawn/ack uses those exact persisted intents. Nested
forwarding uses the intent/child-receipt/root-ack protocol in section 4; it never claims an atomic
multi-file transaction. Failure before durable commit has no transition; uncertain commit is
resolved by receipt observation and cannot be submitted as a new effect.

Error payload: `error:{code,message,data:{request_id,episode_id,current_revision,
current_interaction_epoch,current_target_ref,receipt_ref,retryable,next_action}}` inside validated
ctl envelope; MCP retains same structured code/data through its error transport. `retryable`
means safe same-request observation/reconciliation only, never permission to retarget prose.
No current target/receipt disclosure to an unauthorized principal.

| Codes | Meaning / next action |
| --- | --- |
| `steering_unsupported`, `steering_role_unsupported`, `steering_budget_unsupported` | Capability/runtime/policy unavailable; no issuance. Use supported subset or obtain policy decision. |
| `caller_undecidable`, `steering_not_owner`, `managed_caller_forbidden`, `interaction_revoked` | No authority; do not look up arbitrary targets or launch. |
| `steering_target_stale`, `steering_revision_stale`, `steering_source_unavailable` | Refresh exact source; explicit retarget only. |
| `steering_busy`, `steering_boundary_pending`, `steering_delivery_unknown` | Accepted/held versus refused indicated explicitly; observe same identity, no interrupt/replay. |
| `request_payload_conflict`, `request_expired`, `steering_queue_full`, `steering_budget_exhausted` | Preserve original receipt/state; no silent new request or allowance. |
| `commit_frame_required`, `malformed_command`, `conflicting_command`, `role_command_invalid` | Persist refused output; zero graph changes; next correction needs explicit caller request. |
| `steering_question_pending`, `steering_approval_pending`, `workflow_terminal` | Use actual existing interaction/control endpoint; steering supplies no answer/grant. |

## 8. Projections, events, compatibility and fixture delta

Native source inspection additionally requires `WorkflowRunPolling` fixed state keys AND monitor
digests to include steering changes, otherwise cached status can hide them. The current Activity
composer requires an issued task/child and refuses unissued targets; add explicit checkpoint
steering target mapping. The current builder composer calls legacy builder-followup: integrate
the new operation explicitly, without reinterpreting legacy queued messages.

Full run documents add optional `steering` qualified state; compact/status/list/monitor projections
add optional `steering_summary` with episode/state/revision/hold/reply reference and
`pending_steering_count`. Add explicit field vocabulary/detail view `steering`, and preserve new
fields through MCP compact filtering. Root reads derive nested pending steering from verified
links under one bounded read budget; missing source produces unavailable diagnostics, not an
answerable entry. Steering never appears in `pending_inputs` or increments question counts.

For initial backward compatibility, an idle steering-held run projects existing `paused` plus
an honest reason `Conversation is waiting for caller; use steering capability`, and normal
resume/recover rejects `steering_active` while a non-dismissed or unsettled steering hold remains,
before instructions/grants are applied. Settled explicit dismissal clears only that hold as
defined in section 4; ordinary recovery still verifies all independent blockers. With active
siblings, retain truthful running/settling status and expose hold separately. Do not mark a
workflow completed when only its conversational harness completed. Legacy cancel remains real
terminal cancellation under existing authority, never dismissal. Older readers can observe the
pause/reason or unknown additive state but cannot safely drive it. Role capability plus updated
consumer is required for composer steering; unknown optional fields alone do not prove support.
A builder's prior completed proposal remains historical while active conversation has a paused
run; applying an old proposal must still compare the draft/preview source CAS.

Add normalized event kinds `workflow_steering_requested`, `workflow_steering_turn_reserved`,
`workflow_steering_reply`, `workflow_steering_command_refused`, `workflow_steering_committed`,
`workflow_steering_state_changed`, `workflow_steering_owner_changed`. Each carries IDs, sequence,
role, revisions, outcome/reason and bounded text reference/digest, never secrets or whole oversized
prompts. Keep 16,384-byte event/262,144-byte page limits. A transaction's durable steering ledger
is authoritative; append/notification loss is surfaced and repaired idempotently by event ID,
not by repeating execution. New Mac event kinds map with unknown-kind fallback maintained.

Later producer fixture bundle must add ctl open/queued/discussing/awaiting/replay/conflict/stale/
unknown/unauthorized/unsupported, role-specific builder/orchestrator/worker projections, normal
paused/question unchanged, ownership reconnect/revocation, nested unavailable, exact successful
command and malformed output cases; normalized events; version-info capability subsets; MCP
input/output schema captures; persisted run qualification negatives; cursor/history gap cases.
Classifier golden corpus includes every row in section 5 plus Unicode/CRLF/duplicate keys and
malformed fencing. Fixtures are not created in ST01. Producer spec/qualification/tests/manifest
must land first, then Swift byte-identical vendoring and Rust pinned independent consumption.
No old consumer compatibility or release qualification claim is made by this document.

## 9. ST04 control boundary

ST02/ST03 refuse cancellation/reselection via prose or steering action. ST04 candidate separate
`workflow-steering-replan` / `replan_workflow_steering` requires exact episode, expected epochs/
checkpoint/goal, attempt IDs and descendant scope, reason, proposed configured continuation refs
and idempotency identity. Public phases: `cancel_requested`, `settling`, `settlement_unknown`,
`ready_to_replan`, `replan_committed`. Cancellation permission is independently checked; a pending
approval cannot be self-answered. Persist cancellation intent before signals; use captured stable
process identity/qualified adapter, never discover a new pgid and signal it.

Prestart withdrawal requires positive proof no spawn occurred, atomic intent revocation and
matching resource/message/attempt release. Spawn/ack race or escaped descendants stays unknown.
Poststart replacement requires observed exit AND qualified capture/drain/containment settlement
and no conflicting holder; human manual resource release is not settlement proof. Completed
parallel branches/results/artifacts/checklist evidence remain immutable; retain cancellation
and failed effects. A replan creates a new linked decision/attempt with `supersedes` and preserves
old accepted history, definition snapshot and goal revision. No unaccept, terminal-run resurrection,
rollback or live adoption. A rejected graph/permission change needs a separate real definition/
goal/authority operation and fresh observation. Final schema acceptance for ST04 requires its
adapter-specific settlement proof; this spec does not invent a generic successful cancel receipt.

## 10. Independent deterministic acceptance model

Model state is `(root epoch, target revision, hold, request receipts, issuance ledger, claim set,
accepted command IDs, attempt intents, observed effects, resource holders, draft revision)`.
Inputs are caller requests, child observations, durable writes, spawn/ack/final delivery and crash.
A pure reference model independent of production helpers enumerates interleavings at barriers;
fixture-process external spawn/effect/exit ledgers and public fresh reads are the effect oracle.
Use existing `conformance/s08/observer.py`, `conformance/s09/oracles.py`, rendezvous infrastructure
and tests for dispatch reconciliation, shared interactions, input tree, builder authority and
protocol repair as starting seams, not proof these new cases already pass. No arbitrary sleeps.

Invariants: one accepted commit per turn; no graph advance from prose; no provider turn without
an explicit request; no release/replay from unknown; no managed grant expansion; accepted history
and completed siblings retained; issuance/provider costs never refunded on output classification;
all text verbatim; source/owner/goal/assignment/draft CAS precedes effects. Conversation success
is never inferred solely from content. The test writer must not use production classifier/
normalizer or production reservation state as its expected-result oracle.

| Family and deterministic ordering | Required independent observation |
| --- | --- |
| Admission before/after decision validation, acceptance, child reservation, resource grant, spawn and ack | Admission winner invalidates unaccepted response; accepted decision wins stale refusal; external spawn count matches only accepted intents. |
| Duplicate same/different payload; concurrent native/MCP; response lost at every write boundary | One receipt/issuance/commit; conflict on changed payload; stale loser; replay never adds spawn. |
| Crash before/after admission, turn claim, reservation, spawn intent, observed spawn, ack, final capture, reply, commit, nested child receipt, root ack, event append | Same durable intent resumes observation; no second dispatch on unknown; stored reply survives; accepted commit/event publication does not re-execute. |
| Stale goal, graph snapshot, branch completion, assignment, draft preview, question/approval, root/source link, owner epoch | Zero command effects; exact stale reason; explicit retarget keeps prior text/evidence and budgets. |
| Classifier corpus, quoted JSON/IDs, nested/fenced examples, malformed/duplicate/multiple commands, failures with valid-looking command | Reference outcomes from section 5; no substring trigger or invalid-to-prose downgrade. |
| No-steering plain prose/JSON; ordinary queued context with steering-like text | Existing protocol failures/validators unchanged; queued text never opens episode. |
| Separate turn/provider cost; not-started/failure/unknown/fallback; exhausted decision/node allowance | Issuance monotonic; no refund; provider cost provenance/unknown reservation; separate finite response reservation; no hidden execution retry. |
| Disconnect/reconnect same external process; new process generation; managed task masquerading via ctl/MCP; native takeover/revoke during turn | Same identity can read receipt; new/unknown refuses; revoked output cannot commit; no leaked root metadata. |
| Verified nested target, foreign/cyclic/missing links, partial parallel completion | Only bound source controlled; unavailable remains unknown; already completed branch preserved. |
| Worker tools/assignment/graph/root-checklist abuse; successive own builder previews, replayed preview response, older commit witness, external draft/source edit, old Apply/Save | Existing privileges enforced; tools may have real effects; proposal CAS and explicit Apply/Save remain; zero root authority from conversation. |
| Unsupported runtime/session/cap, capture timeout, containment unknown, cancellation while turn active | Explicit refusal/unknown; no replacement, invented success or unsupported flag. |

Concrete failure traces to preserve as regressions:

1. Orchestrator emits Claude choice; caller admission wins lock; old response validation finishes.
   Fresh acceptance CAS refuses it; no child is spawned. Conversation receives configured choices.
   Prose ends the turn; no automatic second turn. Later explicit reply yields a current valid
   branch bundle and exactly one accepted transition.
2. Decision reservation wins first and spawn ack is lost. Steering request with old snapshot
   refuses stale. A fresh safe-boundary request can wait but cannot replace Claude. Restart
   observes the same intent; `delivery_unknown` keeps resources/claims. No second agent appears.
3. Caller A's prose turn launches; native B takes interaction ownership. A's framed final arrives.
   Persist output and real usage; reject revoked epoch. No graph transition, no refund, no killing
   siblings. B must explicitly request a fresh current turn after settlement.
4. Builder emits ordinary prose after having published a draft preview. Persist reply without
   a final proposal, auto-save or Apply. A concurrently edited canvas invalidates preview/draft
   bindings. A later framed proposal must match latest draft; Save still checks saved baseline.
5. Sibling completes while another activation awaits steering. Its result/effects persist and
   change checkpoint choices; old conversation becomes stale. Retarget explicitly acknowledges
   the completed branch. ST04 cannot re-run that branch as though cancellation rolled it back.

## 11. Bounded implementation briefs and material decisions

| Stage | Entry dependencies | Concrete completion / acceptance |
| --- | --- | --- |
| ST02 builder | Owner/primary accepts ST01 proposed authority policy A and grammar/state; explicit implementation assignment | New shared service, qualified persistence/idempotency/budgets/ctl+MCP read+builder capability, no-auto-loop steering route; builder draft-only proposal path and Apply/Save tests; deterministic model and fixtures. Do not alter ordinary create autosave semantics. |
| ST03-A orchestrator | Accepted ST01 and ST02 evidence | Safe boundary/hold integration through root/nested decision reservations; strict normal protocol unchanged; configured actions only, revision/commit races, no duplicate spawn; canonical producer fixtures and agent MCP semantics. |
| ST03-B worker | ST03-A evidence and explicit owner policy B | Separate durable response accounting, backend-qualified finite caps, immutable assignment/activation validation and real side-effect disclosure; no sibling/root authority. No lifetime steering-count quota. |
| ST04 replan | ST03-A/B and accepted scoped settlement/control policy per adapter | Exact prestart withdrawal and postsettlement linked attempts, parallel preservation, unknown nonreplacement, independent process/effect oracles. Unqualified containment blocks only the affected cancellation mode. |
| ST05 native suffixes | Implemented accepted producer subset + immutable fixture identity for each role/control slice | New typed ctl calls, fresh role gates, optional projections/event mapping, existing transcript/composer/status states, stale draft preservation and concurrent response tests; no MCP/private state fallback. M07/M08/M09/M12 carry acceptance. |
| ST06 Rust suffixes | Accepted producer subset plus relevant R07/R10/R11 foundations | Independent application/domain state and authority/ctl/MCP mapping, pinned fixtures, same deterministic behavioral model and external effect oracles; R11/R14/I04 gates include it BEFORE O04 transfer. Can consume accepted role subsets incrementally. |

Material decisions requiring adjudication are narrow: A (local process-bound external eligibility,
shared native responder and explicit monitor-only revocation; restarted external enrollment remains
unsupported), the accepted B/shared-window amendment above, and ST04 adapter settlement/
cancellation authority before that phase. Hard budget enforcement may exclude an adapter until
qualified; accepting weaker estimate-only spend limits would be a separate explicit policy change.
Routine schema names, separate paused/steering projection, strict frame grammar and conservative
stale handling are proposed settled engineering choices, not open-ended research tasks.

No broad visual design prerequisite. Existing transcript displays exact caller/reply messages,
composer issues one explicit typed request, and status/reason displays queued/discussing/awaiting/
stale/unknown. A bounded missing design is required only if a particular native surface cannot
truthfully expose active siblings alongside a scheduling hold, ownership revocation or incomplete
capture; record that exact gap in ST05 rather than blocking unrelated tracks. This spec does not
pin new pixels or claim owner approval of any new visual state.
