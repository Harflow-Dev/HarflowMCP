# S09 — Controlled Python coverage and outstanding acceptance

This is a coverage ledger, not an accepted conformance certificate. Final commands,
results, identities and case hashes belong to [S09](S09.md) and
[S09-checks.json](S09-checks.json). A passing bounded matrix does not close
C0-MC-04, its contributing receipt acceptance, or aggregate C0.

## Requirements and evidence selection

The assignment retains the original engine parity register E01–E22, original
fault/recovery matrix, and revision-2 validation §8 families 1–13 as traceability
inputs. Their engine requirements were read at session entry. During S09 the
owner redirected the architecture to native Mac redesign first, then Rust, Web
later, and a native Windows application much later. Electron replacement was
abandoned. The tracker owns planning-document integration; this receipt does not
rewrite historical requirements or approve a new implementation phase.

Behavioral expectations come from the selected engine's public
[contracts](../contracts.md), [workflows](../workflows.md),
[resources/worktrees](../resources-and-worktrees.md), and
[shared scratch](../shared-scratch.md) guides. No engine implementation helper is
an outcome oracle. Internal optimized control-delivery version 2 is an explicitly
selected fixture protocol, not a new public wire contract.

| Evidence | Exact selection | Boundary |
| --- | --- | --- |
| S07 | Implementation `a70547a10ecffe2b324abc4235fbb4922ffe0b4c`, receipt `95b696e`; source `0f10ad30e28e7da1873685cebe70aef687881723` | Infrastructure, read-only source boundary and standalone process fake; no engine-triggered task qualification. |
| S08 | Implementation `125d37d38b038259c6f0302ce123e708411d50c3`, receipt `b0185be`; source `95b696ef491a49007434d36bf4ba9eed8c1b5b8a` | Nine controlled task/message/approval cases, actual engine children and registered detached participants. |
| S09 | Source `b0185be855fd13acc94cb2d2008400d285d15df2`; implementation/run identities in the lead receipt | Controlled workflow/resource/statistics matrix below and retained-artifact consistency validation. |

All three selected production `src` trees are
`dddc6a0acf732fde474df397e66291f0f01699b4`, product 0.0.7. S09 includes the
separately owned `db00566` schema/generator/test normalization by selecting the
newer committed archive; it does not attribute that work to S09. The ctl manifest
remains `383ecf7a9b234a1ece18eb15585398f08a77205c02a43cb2a928a2819f76fee0`
with 199 fixtures. Existing Python 3.11.15 Darwin arm64 is distinct from a managed
distribution or any other release. Prerequisite receipts retain their own limits
and pending acceptance.

## Bounded S09 matrix

Each case uses a fresh root and disposable Git workspace. Each public operation
has a fresh capability handshake, bounded captures, strict envelopes and retained
process/capture evidence. Synthetic I/O is untrusted and needs OS/public-state
corroboration. The final receipt records observed outcomes, not this table alone.

| Case | Concrete observations | Boundary |
| --- | --- | --- |
| `definitions` | Validate/save/get/list/delete; stale revision refuses unchanged data; unsupported caller freedom override starts no workflow owner/fake. | No builder Generate/Edit, draft conflict/apply, dependency-cycle/depth or malformed-store matrix. |
| `workflow-complete` | True End completion, settled public attempts and observed integer exits; original run prose; real managed worker's valid public workflow read refuses at the worker-context gate with exact captured PID/argv/diagnostic. | One headless task node, no real provider/native execution, filesystem sandbox or full authority-role matrix. |
| `parallel-nested` | A completed nested child and a still-live held sibling share one workflow owner; release converges; distinct public linked runs/attempts map to actual supervised fake processes. Referenced deletion reports referrers; pinned history survives; missing dependency blocks a new run without execution. | One all-branches split and one child level; not every selection, nested question, retry or Current-mode session policy. |
| `workflow-input` | Wrong decision refuses without changing the checkpoint; exact caller answer is retained and appears in actual control input; completion follows; duplicate answered decision refuses. | One direct root question; no forwarded nested question/caller-loss/structured-choice matrix. |
| `workflow-cancel-recover` | Exact owner self-termination; reads do not relaunch; surviving issued worker stays running with unknown exit/drain. Stale scheduler token refuses; explicit current-token recovery preserves worker/activation IDs; cancel settles supervised participants with observed exits. | Explicit scheduler recovery, not full MCP dispatcher restart, reboot, PID reuse or autonomous owner replacement. |
| `resources-fifo` | One held exclusive resource, two ordered typed pending launches, no queued fake spawn; sequential releases grant/spawn in FIFO order; final inventory is idle and known process exits are observed. | Whole-task default-capacity scope, not phase/service fairness or deep borrowing. |
| `resource-owner-loss` | Dead task supervisor plus surviving fake retains holder/queue capacity; repeated reads retain running/unknown exit/incomplete drain and start no replacement. | Cleanup terminates known registered processes; it does not certify reservation release or reconcile unknown outcomes. |
| `scratch-phase-refusals` | Active shared-scratch cleanup refuses; after observed settlement explicit cleanup removes the owned chain and preserves an unrelated sentinel. Empty worktree inventory/read-only worktree request refusal; layout-1 slot status returns `phase_slots_not_enabled`. | No worktree/branch creation or removal under this authorization; no layout-2 service/acquisition/cleanup qualification. |
| `statistics` | Qualified empty history, completed workflow/root cohort, known terminal standalone executions, active exclusion, full same-generation limit-one paging, expired-cursor refusal, null versus synthetic reported cost/model, genuine managed read refusal. | Retained fresh history, not cross-date/large-history/retention/native-cost or all incomplete-source cases. |

Orchestrator task prompts can be original prose or checkpoint display labels.
Actual control input is associated using the public issued checkpoint and delivery
receipt, session, canonical payload hash and OS identity. Bootstrap original prose
is checked against the run; resumed sessions are not identified by session ID or
display prose alone. Full acknowledged-reference reconstruction remains a gap.

## Revision-2 scenario families

“Partial” means the listed cells have evidence; the whole family is not qualified.

| Family | Evidence | Remaining cells / assessment |
| --- | --- | --- |
| 1. Launch failures | S08 invalid repository/unsupported turn cap; S09 definition/access/dependency refusals. | Missing binary, real protocol initialization and comprehensive pre/post-spawn failure windows remain open. Partial. |
| 2. Concurrent starts/duplicate prevention | S08 overlapping session resume refusal; S09 real parallel branches and preserved issued work during scheduler recovery. | Simultaneous same-session races and uncertain duplicate reservations remain untested. Partial. |
| 3. Owner death/reboot/PID reuse | S08 task owner; S09 workflow/resource owner self-termination with external exit proof. | Reboot, reused/unrelated PID generations and power-loss durability remain unqualified. Blocks full family acceptance. |
| 4. Cancellation/descendants/drain | S08 task and S09 registered workflow chains, public terminal/drain evidence and OS exits. | Escaped/unregistered descendants, arbitrary child trees and engine-integrated pipe stalls remain open. Partial. |
| 5. Continuation/resume/delivery uncertainty | S08 queued/now/cancel/consume/verbatim messages; S09 exact input and explicit scheduler recovery. | Lost responses after possible commit, uncertain delivery reconciliation and all fresh-continuation variants remain open. Partial. |
| 6. Parallel/nested workflows/questions/recovery | S09 bounded split/child/root-input/scheduler recovery. | Nested forwarding, caller loss, decision/dispatch split, retries, on-demand specialists, fallbacks, goals and full context-reference behavior remain open. Partial. |
| 7. Stale/duplicate approvals | S08 exact action/digest/revision/channel and same-action next request; S09 question/checkpoint identities are separate evidence. | Concurrent responders, expiry/owner-change races and lost decision delivery remain open. Partial. |
| 8. Permission/unsupported combinations | Synthetic capability refusals, managed workflow/statistics boundaries, worktree refusal and phase-disabled status. | Real backend freedom/network/native combinations and OS enforcement remain separately unqualified. Partial. |
| 9. Slot fairness/holder death/cleanup | S09 whole-task FIFO, retained holder uncertainty and scratch cleanup. | Phase layout-2 service, capacity changes, borrow anchors, repair and uncertain release remain open. Partial. |
| 10. Authorization-scoped reads | S08 managed approvals; S09 worker workflow context and managed statistics refusal. | Full orchestrator/builder/cross-tree read matrix and filesystem/cache-access tracing are not established. API refusal is not filesystem isolation. Partial. |
| 11. Large histories/bounded paging | S07 infrastructure/fixture controls; S09 small real statistics pagination and stale generation. | No scale, retention/pruning, partial ownership, missing-history or performance qualification. Partial. |
| 12. Backend quirks | S07 process fake and S08/S09 explicit synthetic protocol adapter. | No stock or real Claude/Codex/Vibe/other provider qualification. This family remains open. |
| 13. Client reconnect/engine upgrade | Historical contract/decoder receipts retain their own scope. | S09 has no client, installation, upgrade or registered-runtime experiment. Remains open. |

## Original engine parity and fault requirements

| Original engine rows | Contribution | Open acceptance |
| --- | --- | --- |
| E01–E08, E11 | Carry forward S04–S08 public-read/fixture/task/message/approval evidence; S09 adds selected authorization and task-drain observations. | MCP execution/schema partition, setup, all lifecycle/fault variants, artifacts and full event/history coverage are not closed here. |
| E09 Resources | FIFO, holder uncertainty and observed release progression. | Borrow/repair/phase/service and uncertain settlement variants. |
| E10 Worktrees/scratch | Safe scratch guard/cleanup and worktree refusal/inventory. | Worktree creation/removal, shared workflow checkouts, pin/artifact/retention combinations remain open; branch/worktree mutations excluded. |
| E12 Definitions/builder | Core definition round trip/revision and referenced deletion/pinned history. | Builder proposals, draft apply/conflicts, full dependency qualification. |
| E13–E14 Execution/control | Simple task/parallel/child run, exact input, explicit scheduler recovery and cancellation. | Advanced branches/retries/budgets/fallbacks/on-demand, recordless recovery, every control state and owner replacement. |
| E15–E16 Context/attention | Issued context-receipt/input identity and direct answer delivery; managed node refusal. | Full references/append/cache reconstruction, goals/checklist, nested question/attention routing and caller-loss handover. |
| E17 Workflow infrastructure | Real shared workflow owner, linked child identities and known process cleanup. | Run resource borrowing, shared worktrees, root notification semantics and deeper nested activity/authority. |
| E18–E20, E22 | No new acceptance claim. | Notifications, setup/client integrations, privileged controls and store recovery require separate evidence/authority. |
| E21 Statistics/retention | Known cohorts, cost/model unknown semantics, small full paging and stale snapshot. | Cross-date cohorts, native transport deduplication, missing/corrupt ownership, retention, large history and bounds/performance remain open. |

Original fault-matrix coverage is likewise partial: one issued-worker scheduler
recovery and known-process cancellation do not test all commit/spawn/ack windows,
MCP restarts, PID reuse, partial UTF-8/pipe pressure, disk exhaustion, I/O/corrupt
storage, response loss, cancellation/withdrawal races, nested dispatch split,
resource borrow/repair, notification leases, import/checkpoint interruption or
active runtime-pointer changes. An intermediate harness registration refusal is
retained diagnostic evidence, not a systematic fault-family pass. Rust-only
authoritative-SQLite/import gates remain future requirements; Python cache reads
do not satisfy them.

## Acceptance and handoff

C0-MC-04 remains unchecked/pending. The supplied source-injection profile provides
bounded Python observations. The owner still needs to accept the relevant scope,
runtime/profile and contributing receipts. Required process/fault and behavior
cells above need implementation or an explicit scope decision; S09 does not
silently waive them. S03 Proposed/Undecided rows, including the exact 47/eight MCP
candidate partition, remain unresolved by this session.

The workflow/resource suite is currently Python source-injection specific. S07's
binary adapter has only stub/read-boundary checks; no generic binary-target
workflow/resource adapter or Python/Rust differential execution is certified.
Python stdlib/system libraries and external editable dependency closure remain
unpinned. Same-UID cooperative evidence is not a tamper-proof attestation.

S09 evidence can inform the tracker's next design reconciliation under the owner's
native-first direction. It is not authorization to begin S10, implement UI/build
systems, select a Rust runtime or claim real-client readiness. Current engine
guarantees remain required despite deliberate UI removals. No migration planning
file is updated by S09. Historical Mac failures retain the freeze waiver and
unknown baseline origin; no Mac rerun or all-green native claim is supplied.
