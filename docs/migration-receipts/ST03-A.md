# ST03-A — Orchestrator conversational steering

Date: 2026-11-05 (Asia/Jakarta). Lead: HarflowMCP.
Status: implemented and independently reviewed; final strict unit-only selected profile passed
753 tests, with two live-root tests deliberately deselected. No owner acceptance, release or consumer adoption.

## Assignment and exact inputs

The owner explicitly authorized ST03-A in the existing steering chat. This supersedes ST02's stop
boundary only for orchestrator steering. ST03-B worker policy, ST04 cancellation/replan, native/Rust
implementation, installation, push/publication and release remain unassigned. Local cohesive commits
are authorized. Primary checkout only; no new branch or worktree. Tracker-owned migration documents
and the three `dist-rc/` files are excluded and independently hash-preserved.

Input HEAD: `d71fe727d2ffff33e60fdeafaf00bb78038dfb2d` (ST02 final receipt), following implementation `411868b` and validation inventory
`d51ad8a`. ST02 reviewed production source tree: `ea2a5ba4d89c31373d1b552f9de8eee9f6767f0c`.
Input producer manifest SHA256: `b7cdb005c335d6b380d23b2a35cbe4fb13ef1f6fadce916a390880536f0cc812`
(219 files). [ST01](../decisions/ST01-steering.md) and [ST02](ST02.md) are the specification and actual
shared-service inputs. Accepting this evidence for bounded implementation does not establish
released/runtime support or consumer acceptance. Product metadata remains unreleased source 0.0.10.

## Implementation and review

The primary inspected the current checkout and formed a bounded plan outside the product repository.
A fresh native read-only architecture/plan review identified shared root lock seams, ordinary
acceptance bypasses, nested/shared-session boundaries and accepted-work preservation. The primary
adopted those recommendations. Production implementation uses a native `gpt-6.1-sol` low-effort
worker because concurrency, authority and lifecycle uncertainty remain. A separate native test
writer owns independent synthetic fault/effect regressions. The primary owns integration,
canonical fixtures/docs, independent checks, adjudication and commits. No Harflow dispatch or
external provider delegation is authorized by this assignment.

Implemented behavior:

- Shared ctl/MCP operations expose exact root and reciprocal nested decision targets. New capability
  `workflow-steering-orchestrator.v1` is additive to read/builder capabilities and existing wire v1.
  Root ledger owns requests, issuances, history, control epoch and command intents. Managed callers
  remain refused before target disclosure; original external generation/native-human policy A remains.
- Admission, decision acceptance and new effect reservations share the root tree mutation lock.
  Admission first supersedes only unaccepted ordinary output; accepted decision first stales the old
  target. An affected accepted assignment without its execution reservation refuses admission with
  `steering_boundary_pending` rather than withdrawing issued work. Target subtree/new root decisions
  and shared-owner control sessions are fenced; issued work and active sibling outcomes remain real.
- Active ordinary control generations are queued/observed exactly. One explicit request permanently
  charges one finite capped issuance, independently of normal decision/execution attempts. No provider
  fallback, repair, implicit correction, conversation loop or refund. Unknown spawn/capture retains
  its identity/hold and is never replaced.
- Positive final capture precedes classification. Prose waits. A strict whole-final role command
  revalidates exact target/epoch/revisions and original configured-action validators. Optimized mode
  uses a dedicated full bootstrap and exact v2 context acknowledgement. Continue/complete/fail/input,
  inspection and configured specialist actions retain their original authority, choices and budgets.
  Steering inspection/specialist issuance does not refund an ordinary decision attempt.
- Nested admission and action acceptance use durable root intent, exact child receipt and root
  acknowledgement. Lost response or owner restart reconciles only the same intent/receipt. Strict
  schema qualification and runtime checks reject incomplete, forged or contradictory receipts.
- Idle conversation uses a real paused checkpoint; active issued tasks/siblings retain truthful
  running/settling observations. Capture does not overwrite concurrent cancellation/question/failure
  states. Dismissal is explicit and schedules nothing. Native ownership changes invalidate command
  authority without killing a harness. No fabricated question/attention identities or normalized
  steering event capability was added.

Primary self-review found malformed/incomplete orchestrator targets could escape typed error
handling; four regressions failed before structural qualification and passed after the correction.
This check precedes target resolution and creates no issuance. Independent final review round 1 identified the following defects; primary adopted all four.
Round 2 closed findings 2–4 and terminal/question-state preservation. It retained the active-sibling
part of finding 1: running/prestart dismissal cleared the hold too early. A shared dismissal helper
now rechecks issued siblings under the root mutation lock, keeps `dismiss_pending_settlement` until
observed settlement, then clears into the real paused checkpoint without scheduling. Reserved-turn
and genuinely running-before-capture regressions cover this boundary. Round 3 closed all four
findings with no additional actionable findings. The final combined run below follows that closure.

| Finding | Correction |
| --- | --- |
| Dismissal could overwrite terminal/cancellation/question state | All dismissal paths preserve real state/reason; active siblings keep their real running task and defer hold settlement. Builder cancellation cannot be resurrected. |
| Any same-token task history counted as the accepted worker reservation | Require the exact current node execution/assignment/decision generation and real reservation; completed decision-provider and historical retry tasks do not qualify. |
| Prose/reply could silently slide a stale episode target | Stale prose retains output/usage as stale; reply binds the same episode witness. Fresh goal/graph/owner/child targeting requires explicit retarget. |
| Reciprocal IDs omitted nested pinned-source/session-owner qualification | Validate configured parent node/ref/dependency edge, pinned definition/hash/revision/subtree/mode and the exact derived nearest session owner/configuration before advertising or admitting. |

Synthetic nested fixtures were corrected to include the revision/hash that real invocation publication
already emits. No qualification check or effect assertion was weakened to accommodate those fixtures.
The independently authored review regressions ran after the fixes landed; no original-red claim is made.

## Observed verification

Final primary run on the frozen, reviewed source: **753 passed, two deselected in 324.12 s**.
Collection proved 753 selected items, both integration flags absent and zero forbidden markers.
All 188 Python source file hashes matched before and after execution. This is the audited affected
profile across 28 files, not a new full-repository, installed-runtime or real-provider qualification.
It includes the 55 core tests, 89 independently authored fault cases, four malformed-target
regressions and existing orchestration, builder, restart, authority, schema, ctl/MCP and fixture checks.

Two exact tests remain deliberately excluded:
`test_wire_contracts.py::test_version_info_uses_the_same_effective_home_as_path_home_without_state`
consults the live passwd home, and
`test_workflow_attention_checkpoints.py::test_public_mcp_workflow_controls_expose_checkpoint_without_changing_question_path`
can open the real MCP notification lifespan. All selected new/existing bodies and indirect helpers
were audited: fake registries, temporary roots, headless configurations and isolated fixture
shell/process shims only. No real provider, registration, acquisition, vendor configuration or
live-root action was used for these checks.

Selected profile at this source (the execution additionally used a temporary fail-closed collection
guard to check unset flags and selected markers before any test ran):

```sh
env -u HARFLOW_INTEGRATION -u HARFLOW_CLI_INTEGRATION -u HARFLOW_NOTIFY_DESTINATIONS \
  .venv/bin/python -m pytest -v -m 'not integration and not cli_integration' \
  tests/test_orchestrator_steering_*.py tests/test_workflow_steering_*.py \
  tests/test_workflow_on_demand_*.py tests/test_workflow_delegation.py \
  tests/test_workflow_protocol_repair.py tests/test_workflow_goals.py \
  tests/test_workflow_supervisor_process_recovery.py \
  tests/test_workflow_recovery_supervision_regressions.py tests/test_wire_contracts.py \
  tests/test_approval_seams.py tests/test_workflow_run_documents.py \
  tests/test_workflow_receipt_documents.py tests/test_workflow_shared_interactions.py \
  tests/test_workflow_interactions.py tests/test_workflow_attention_checkpoints.py \
  tests/test_workflow_attention_cleanup.py \
  --deselect=tests/test_wire_contracts.py::test_version_info_uses_the_same_effective_home_as_path_home_without_state \
  --deselect=tests/test_workflow_attention_checkpoints.py::test_public_mcp_workflow_controls_expose_checkpoint_without_changing_question_path
```

Both handshake and steering fixture generator `--check` runs pass. The latter checks the complete
synthetic producer output twice in the selected tests. Private-reference, legacy-name and whitespace
guards pass. The original 55 non-steering MCP tool schemas remain exactly equal to the input. The three existing
steering tools retain their signatures; maintained steering descriptions were updated. Total
inventory stays 58.
Five protected original files retain their captured hashes; added local documentation links resolve.

Earlier primary checks are diagnostic: 680 passed / two deselected before the final corrections;
144 passed after round-1 fixes and 149 passed after the last dismissal fix. Another broader run
returned 748 passed / two deselected in 255.11 s, but a source correction landed during it, so it
is mixed-source evidence and is not used for final qualification. Only the final 753-test run above
qualifies the frozen final source.

## Producer output identity and remaining gates

Producer implementation commit: `7c527d8fe060ed00272d6207a4cecfc8bf5b2aef`.
Producer source tree: `95a81192756f2caa4642d1e46ebd25b9144bc4a3`.
Complete canonical bundle: **235 files**, manifest SHA256
`66e06af29b195472e42215676e8feb1028265c280a7aa08e5f54f39a55e36cd5`.
These immutable identities cover source and synthetic contracts, not a published engine artifact.
Product metadata remains 0.0.10; the released tag does not contain this capability. The receipt metadata follow-up changes no production source or contract bytes.

Swift adoption requires independently assigned byte-identical complete fixture vendoring and typed
ctl integration. Rust independently implements the same pinned accepted contract before consumer
integration. Historical, missing or contradictory nested provenance refuses rather than inferring
source/session authority. External application restart enrollment, worker steering/tool policy,
scoped cancellation/replan, normalized steering-event semantics, consumer adoption, owner acceptance,
CI/platform/real-backend qualification, publication and runtime selection remain separate gates.
No sibling checkout or selected engine/runtime changed. Stop after ST03-A.

## Validation boundary

Both `HARFLOW_INTEGRATION` and `HARFLOW_CLI_INTEGRATION` must be UNSET, never set to `0`. Every
pytest invocation excludes `integration` and `cli_integration` markers explicitly. Before broad
execution, a collection gate proves both flags absent and zero forbidden selected markers; selected
new test bodies/helpers are inspected for direct real CLI/provider/registration/config effects.
The prior ST02 invocation incident remains recorded in its receipt and grants no permission to
repeat it. Only isolated fake registries or synthetic fixture processes are permitted here.

No real-backend, installed-runtime, consumer, UI, CI or release qualification is implied by unit
checks. Producer contracts land first; later Swift adoption requires byte-identical complete fixture
vendoring and explicit assigned integration, and Rust independently implements the pinned accepted
contract. No sibling checkout or selected engine/runtime is changed by this assignment.
