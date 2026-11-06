# Python steering readiness for Mac

Canonical owner: HarflowMCP. This is an implementation handoff, not a release,
installed-engine qualification or native acceptance. Mac continues to use public
`harflow-ctl` and a freshly observed version/capability handshake.

| Operation | Producer contract and gate | Native implementation boundary |
| --- | --- | --- |
| Steering status/history | `workflow-steering-read.v1`; bounded status/history responses | Consume canonical fixtures, epochs, cursors and explicit history gaps. Do not read private persistence. |
| Builder conversation | `workflow-steering-builder.v1`; qualified root and caller | Show queued versus issued responses; submit exact targets/revisions and explicit messages. Active builder queueing does not authorize interruption. |
| Orchestrator conversation | `workflow-steering-orchestrator.v1`; qualified target | Preserve checkpoint identity and accepted decisions. Do not infer replanning or execution authority from conversation. |
| Worker conversation/window | `workflow-steering-worker.v1` and `workflow-steering-window.v1`; schema2/current epoch and qualified worker profile | Preserve one finite response per explicit message, budgets and unknown usage. Stale/expired requests refuse; do not auto-retarget or retry. |
| Prestart withdrawal and linked retry | `workflow-steering-replan.v1` plus worker/window; exact engine witness; root connected fixed headless/Fresh profile | Offer only engine-issued `replan_targets`. A spawn race or missing settlement proof stays unknown. Never convert refusal into task cancellation. |
| Poststart containment/replacement | `workflow-steering-poststart.v1` remains reserved and unadvertised; production facade refuses | No actionable stop-and-replace control. Candidate/measurement artifacts cannot enable this operation. |

The normative request, caller-authority, response and error rules are in
[contracts](../contracts.md#builder-conversation-steering), including
[worker windows](../contracts.md#worker-steering-and-bounded-windows) and
[poststart specification](../contracts.md#poststart-steering-candidate-specification).
UI must preserve cancellation/unknown/queued distinctions, original accounting and
explicit ownership rules. Current source support is not evidence that a selected
published engine version advertises these capabilities.

The [checkpoint receipt](ST04-WORKFLOW-CHECKPOINTS.md) records completed private
Python conformance. The [fixed-helper qualification contract](../decisions/ST04-helper-qualification.md)
keeps physical OS/capture/writer measurements separate from Python workflow facts.
Unpaid helper success does not qualify a real coding backend, authorize replay
of unknown effects or accept a generic repeat-safe file-edit profile.

Mac can implement the supported operations above against pinned canonical fixtures
and explicit unsupported states. Enabling production poststart requires a real
accepting Python route with qualified original launch, capture/writer closure,
trusted old/successor effect verification, caller/configuration/budget checks,
then public capability/fixture delivery. It remains a separate unsatisfied readiness
item until those conditions are observed; no source-only or helper-only receipt
may mark it complete. Rust adoption and its runtime qualification remain separate.
