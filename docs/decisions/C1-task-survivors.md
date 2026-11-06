# C1 candidate: named survivors and slot retention

Candidate `task-survivors-v1`, reserved capability `task-survivors.v1`. Addresses MCP #118: cancelling a
task leaves its `xcodebuild`/Gradle builds running and releases the heavy-build slot early. Specified,
unimplemented and **not advertised**; `contracts/v1`, `version-info` and Python behaviour are unchanged.
Assets: `contracts/candidates/task-survivors-v1/`.

Basis: the engine completion plan, decision **D12** (report named survivors; retain the slot until
settled or a human explicitly releases it; a release never implies settlement or authorizes replay;
signal only generation-checked identities). **D12 is a recommendation; the owner approved D1 to D11
only.** This candidate is therefore a proposal and says so in its manifest (`plan_D12:
recommended_not_approved`); the explicit-confirmation flag `confirm_unsettled` below is also my proposal,
not an owner decision. It builds on the owner-runtime containment, writer and claims evidence
([owner-runtime-v1](owner-runtime-v1.md), "Independent result, obligation and settlement evidence") and on
the existing phase-slot and resource ledgers ([contracts](../contracts.md), "Task phase slots"); it
changes neither.

## Survivors report (`kind: report`)

An additive optional `survivors` object on a task (cancel results, `status`, slot rows):
`{state, inventory, basis, items, truncated}`.

- `items` (at most 32): `{name, identity, state}`. `name` is a display string of 1 to **64 UTF-8 bytes**
  (not characters; 32 two-byte or 21 three-byte characters fit, one more does not), not authority.
  `identity` is the process identity of the owner-runtime schema (PID, start seconds and microseconds,
  boot UUID) plus the 32-hex `generation` of the attempt that spawned it. `state` is `alive`, `dead` or
  `unknown`.
- `inventory`: `complete`, `partial` or `unknown`; `truncated: true` requires `partial`.
- `basis`: the evidence the inventory rests on, from `process_group`, `session`, `descendant_tree` and
  `containment_complete` (the owner-runtime `containment: complete` observation). **`inventory: complete`
  requires at least `process_group`, `descendant_tree` and `containment_complete`**; any smaller basis
  must report `partial` (or `unknown`). A scan of the task's process group alone, which misses a daemon that
  started its own session, therefore can never reach `complete`.
- `state` is derived, and a report that disagrees is invalid: `present` when any item is `alive`;
  otherwise `unknown` when the inventory is not `complete` or any item is `unknown`; otherwise
  `none_observed`. `none_observed` therefore always rests on a complete inventory.

**Cost.** The Engine does not claim arbitrary descendant containment on macOS (R01 selects session and
process-group control only). If `containment_complete` and a descendant tree cannot be attained there, no
inventory is ever `complete`, no cancel yields `none_observed`, and **every cancel holds its slot until a
human acts**. That is the safe direction and it is the consequence of D12 plus this evidence rule; the
alternative (accepting a process-group scan as complete) would reintroduce #118. The owner should choose
knowingly.

## Slot retention (`kind: retention`)

The projection of what a phase or whole-task slot does after a cancel or exit. Inputs: the survivor state and
inventory (consistent with the report: `none_observed` needs a `complete` inventory), the slot, whether a
human asked to release, whether that human confirmed that survivors are unsettled, and the **settlement
evidence** of the owner-runtime model: `containment` (`complete`), `writer` (`settled`) and `claims`
(`reconciled`).

| Situation (first match wins) | `slot.state` | `reason` | `settlement` | `replay` |
| --- | --- | --- | --- | --- |
| human release without confirmation | `retained` | `confirmation_required` | `not_implied` | `not_authorized` |
| human release with confirmation | `released` | `human_release` | `not_implied` | `not_authorized` |
| survivors present | `retained` | `survivors_present` | `not_applicable` | `not_applicable` |
| survivors unknown, or inventory not complete | `retained` | `survivors_unknown` | `not_applicable` | `not_applicable` |
| none observed, but containment, writer or claims not settled | `retained` | `settlement_evidence_incomplete` | `not_applicable` | `not_applicable` |
| none observed, complete inventory, evidence settled | `release_eligible` | `no_survivors` | `not_applicable` | `not_applicable` |

`release_eligible` is an observation that the slot may be released through the ordinary lifecycle, never a
release. A retained slot keeps consuming capacity until survivors settle or a human releases it; there is no
timeout or automatic release. (The #118 acceptance criteria ask for a bounded timeout; D12 has none. That
conflict is an owner question.) A human release is an explicit acknowledgement, never evidence that the
survivors stopped, and never authorizes replaying or resuming the task. `confirm_unsettled` is a proposal for
how that acknowledgement is made explicit; the route (D15) is undecided.

## Signalling (`kind: signal`)

Inputs: the `recorded` identity with its `generation`, the identity `observed` now from the operating system
(PID, start time, boot; null when it cannot be observed), and the durable record's `current_generation`.

1. Unobservable identity: refuse `identity_unknown`.
2. Observed PID, start time or boot differs from the recorded one: refuse `identity_mismatch` (covers PID reuse
   and another boot).
3. The recorded `generation` differs from the record's `current_generation`: refuse `generation_stale`.
4. Otherwise permit `identity_current`.

A saved PID or process group is never signalled blindly. **Race:** the check and the signal are not atomic.
Between the identity comparison and the kernel delivering the signal the process can exit and the PID be
reused. `permit` is a necessary condition, not a guarantee; the signaller must re-observe immediately before
delivery, keep that window as short as the platform allows, prefer a handle it owns (a direct child, or a
group it created) over a PID, and treat any signal as best-effort that cannot prove the right process
received it. The candidate does not remove the race and does not claim to.

## Specimens and checks

Same envelope and bounds as the other C1 candidates. The checker recomputes the survivor state, the basis
rule, the slot projection and the signal decision from hand-written inputs, so a document that releases a slot
early, releases without settlement evidence, claims settlement or replay authority, calls a process-group scan
complete, or permits a signal after PID reuse or a superseded generation is rejected. Multibyte name specimens
pin the byte limit.

## Not decided here

- How survivors are detected and named (`xcodebuild`/Gradle are examples from the ledger; the detection
  mechanism and which processes count are undefined).
- The human release route (D15) and whether it reuses `resources-release --confirm-uncertain` or
  `slot resolve`; until D15 Rust refuses release (D8).
- Who may signal survivors and with which signals.
- Whether a bounded timeout (as #118 asks) replaces "no timeout" (as D12 says).
- Interaction with #112 and other attention states, and the Mac presentation.
