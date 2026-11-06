# Conversation delivery

`conversation_delivery_v1` is the negotiated response-boundary delivery contract.
Its runtime/profile qualification is separate from native protocol availability.
An unsupported source refuses before new input; a caller cannot enable support
by supplying a qualification boolean or by editing a delivery request.

Queue lets the current response finish, then automatically delivers one message
for one response, FIFO, in the same native conversation. Tools inside the active
response do not create Queue delivery boundaries. Now selects one message,
requests native interruption of the exact active turn, waits for its terminal,
approval and tool evidence, then delivers that message. Earlier Queue messages
remain queued. An uncertain effect blocks further issuance; Now does not roll
back repository changes or replace the conversation with a Fresh task.

## Caller protocol

Select a qualified control transport when launching with
`harflow-ctl run --conversation-profile native` (or the separately declared
`shipped-medium35-high` profile), or the corresponding `start_task` argument.
An empty or mismatched qualification manifest refuses before launch. A profile
request does not change the requested freedom, network or approval opt-in.
The internal Mac [qualification matrix](decisions/conversation-delivery-qualification-v1.md)
requires explicit caps of 8 or 100 for Claude/Vibe and omitted caps for the other
measured transports. Its model, effort and network selections are omitted;
other selections need their own accepted exact profile.

Private launch-profile evidence is bounded to 256 KiB. An oversized ordinary
profile or initial assignment refuses before native input, rather than truncating
caller prose. Explicit sealed workflow commits keep their existing separate
whole-result grammar and output bound.

`harflow-ctl conversation-prepare TASK --json` explicitly prepares an eligible
target and returns its `expected` witness. Agent clients use
`prepare_task_conversation`. This operation does not dispatch a response.

Submit a UTF-8 JSON file with `harflow-ctl conversation-send REQUEST.json --json`,
or pass the same document to agent MCP `send_conversation_message`:

```json
{
  "contract": "conversation_delivery_v1",
  "request_id": "30e01dfe-98db-4e68-8cf3-272734ec939f",
  "delivery": "queue",
  "text": "Keep the public interface unchanged.",
  "target": {"kind": "task", "task_id": "exact-source-task-id"},
  "expected": {
    "conversation_id": "d5c7671a-b27f-431d-bc70-5d9b6861945b",
    "head_task_id": "exact-resolved-head-task-id",
    "generation": 1,
    "turn_id": "exact-logical-turn-id",
    "ledger_epoch": 0
  }
}
```

Copy the actual witness from prepare; the example identities are placeholders.
A workflow target instead has `kind: "workflow_activation"`,
`workflow_run_id`, `activation_id` and `task_id`. It addresses that exact actor,
never a sibling, later retry or whichever node happens to run next.

Preserve prose verbatim. One request UUID identifies the complete original
intent and witness. Retry a lost reply with that identical request. A different
intent needs a new UUID; changing the text or target of an existing UUID refuses.
Admission is not consumption. Inspect durable message state rather than assuming
that a spawned process or native input acknowledgement proves delivery.

Control writers have a finite idle lifetime. A prepared `closed: true` target
refuses new Queue/Now input; it is not silently reopened. After standalone
physical settlement, use the existing explicit same-session `resume` operation
with its qualified profile and cap. Workflow continuation/commit and builder
Apply/Save remain explicit operations with their existing authority checks.

`conversation-control` / `control_conversation_message` accepts a separate
control UUID, the same `target` and `expected` witness, an existing
`message_request_id`, and `operation: "cancel"` or `"promote_now"`.
Cancellation applies before claim or durable interrupt intent. Promotion keeps
the original message and its replay identity, gives it Now priority, and leaves
older Queue messages in FIFO order. Exact control retries return the original
receipt; reusing a control UUID for another operation refuses.

The bounded history retains exact replay receipts within its rolling window.
Compaction advances `ledger_epoch`; aged-out requests carrying an old epoch
refuse. Outstanding and unknown claims remain protected. Cumulative issuance
and accounting survive history expiry, and unavailable usage stays unknown.

## Activity authority

Standalone settlement owners drive automatic delivery, including detached task
owners. Only the physical process monitor publishes task settlement after exit
and output drain; native response completion alone never sets `Task.done`.
New input and idle closing share an atomic admission boundary.

Workflow admission and result holds share the existing run mutation. A response
to conversational input cannot itself advance the graph or publish a builder
preview. Existing ownership, source draft revisions, explicit commit/apply and
finite response budget checks remain authoritative. Independent siblings retain
their own results and scheduling authority.

After physical source settlement, `conversation-commit` /
`commit_conversation_result` can explicitly issue the existing capped same-session
workflow commit. Its closed document contains `contract`, `request_id`,
`operation: "commit"`, `target`, `expected`, and a complete `steering_request`.
The inner request uses that same UUID, `action: "open"`, the existing complete
steering target/revision witnesses, and `allow_fresh_context: false`. Existing
finite-cap and framed-result validators decide the result. Unsupported commit
profiles refuse; a native response terminal never substitutes for physical exit.
The original conversational source remains fenced. A valid builder commit
proposes an unsaved draft; Apply/Save remains a separate explicit action.

Legacy `send`/`send_message` follow-up records retain their published manual
continuation meaning. They are not migrated or automatically consumed by the
new conversation ledger. Native Now also requires a control transport selected
at launch; a running legacy print process cannot acquire it retroactively.
Native conversation availability comes from the negotiated contract and its
`conversation` status, independently of the legacy `live_input`/input-channel
fields. Transport deadlines bound startup, pipe writes, interruption settlement
and closing; healthy active responses and ordinary tool/effect waits follow the
existing task execution timeout policy.

Only the exact owner-approved internal Mac runtime/profile matrix is accepted.
Other versions, profiles and grant combinations refuse before new input. Now
refuses known active tools whose cancellation/closure is unqualified with
`tool_interruption_unqualified`; a late race remains uncertain and blocks further
input. Native response settlement does not certify OS descendant containment.
Publication and Rust adoption remain separate gates.
