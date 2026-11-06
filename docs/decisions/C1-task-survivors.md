# C1 candidate: named survivors and slot retention

Candidate `task-survivors-v1`, reserved capability `task-survivors.v1`. Addresses MCP #118: cancelling a
task leaves its `xcodebuild`/Gradle builds running and releases the heavy-build slot early. Specified,
unimplemented and **not advertised**; `contracts/v1`, `version-info` and Python behaviour are unchanged.
Assets: `contracts/candidates/task-survivors-v1/`.

Basis: the engine completion plan, decision **D12** (Report named survivors. Retain the slot until settled
or a human explicitly releases it. A release never implies settlement or authorizes replay. Signal only
generation-checked identities.). **D12 is a recommendation; the owner approved D1 to D11 only.** This
candidate is therefore a proposal and says so in its manifest (`plan_D12: recommended_not_approved`). It
builds on the owner-runtime containment and claims evidence and on the existing phase-slot and resource
ledgers ([contracts](../contracts.md), "Task phase slots"); it changes neither.

## Survivors report (`kind: report`)

An additive optional `survivors` object on a task (cancel results, `status`, slot rows):
`{state, inventory, items, truncated}`.

- `items` (at most 32): `{name, identity, state}`. `name` is a bounded display string (1..64 bytes), not
  authority. `identity` is the process identity of the owner-runtime schema (PID, start seconds and
  microseconds, boot UUID) plus the 32-hex `generation` of the attempt that spawned it. `state` is `alive`,
  `dead` or `unknown`.
- `inventory`: `complete`, `partial` or `unknown`; `truncated: true` requires `partial`.
- `state` is derived, and a report that disagrees is invalid: `present` when any item is `alive`;
  otherwise `unknown` when the inventory is not complete or any item is `unknown`; otherwise
  `none_observed`. An empty or partial inventory is never `none_observed`.

## Slot retention (`kind: retention`)

The projection of what a phase or whole-task slot does after a cancel or exit:

| Situation | `slot.state` | `reason` | `settlement` | `replay` |
| --- | --- | --- | --- | --- |
| no survivors observed, inventory complete | `release_eligible` | `no_survivors` | `not_applicable` | `not_applicable` |
| survivors present | `retained` | `survivors_present` | `not_applicable` | `not_applicable` |
| survivors or inventory unknown | `retained` | `survivors_unknown` | `not_applicable` | `not_applicable` |
| human release without confirmation | `retained` | `confirmation_required` | `not_implied` | `not_authorized` |
| human release with explicit confirmation that survivors are unsettled | `released` | `human_release` | `not_implied` | `not_authorized` |

A retained slot keeps consuming capacity until survivors settle or a human releases it; there is no
timeout or automatic release. A human release is an explicit acknowledgement, never evidence that the
survivors stopped, and never authorizes replaying or resuming the task.

## Signalling (`kind: signal`)

A process may be signalled only when its recorded identity and generation are confirmed: unobservable
identity refuses (`identity_unknown`), a differing PID/start/boot refuses (`identity_mismatch`, which
covers PID reuse and another boot), a differing generation refuses (`generation_stale`); otherwise
`permit` (`identity_current`). A saved PID or process group is never signalled blindly.

## Specimens and checks

Same envelope and bounds as the other C1 candidates. The checker recomputes the survivor state, the slot
projection and the signal decision from hand-written inputs, so a document that releases a slot early,
claims settlement or replay authority, or permits a signal after PID reuse is rejected.

## Not decided here

- How survivors are detected and named (the `xcodebuild`/Gradle examples are from the ledger; the
  detection mechanism, name vocabulary and which processes count are undefined).
- The human release route: D15 is open, and until it is decided Rust refuses release (D8). Whether the
  release reuses `resources-release --confirm-uncertain` or `slot resolve` is open.
- Who may signal survivors and with which signals; this candidate only constrains the identity check.
- Interaction with #112 and other attention states, and the Mac presentation.
