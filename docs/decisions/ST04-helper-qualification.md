# ST04 fixed-helper qualification

This is the canonical measurement contract for the owner-approved
[poststart policy](ST04-poststart.md) and
[backend enforcement design](backend-enforcement-qualification-v1.md). It does not enable
poststart control, adopt the contract in Rust or qualify a production backend.
Python remains the engine used by Mac and the canonical wire-contract owner.

## Evidence and authority

Fixed unpaid native helpers run only in newly created physical disposable roots.
The Rust platform/owner harness supplies OS measurements; Python checkpoint tests
supply workflow conformance. Neither diagnostic JSON nor an all-true candidate
certificate supplies live execution authority. No supported endpoint or capability
may be enabled from these artifacts.

Every measurement binds source commits and reviewed input hashes, helper source,
compiler invocation and compiler/binary hashes, effective profile and revision,
OS/build/architecture, argv/environment and original physical root/process identity.
All file/hash reads have explicit budgets and no-follow regular-file checks.
Record the identity before issuance and recheck it after settlement. Bookends are
measurement witnesses, not protection against hostile outside replacement.

The diagnostic schema is separate from the immutable ST04 candidate and supported
contracts. It fixes `runtime_authorized`, `production_qualified` and `rust_adopted`
to false. Its `observed_success` is local to the named obligation and profile.
Unmeasured, unsupported, failed and unknown obligations are never overall success.
The schema is structural only: producers additionally enforce strict UTF-8 JSON,
no duplicate keys/nonfinite numbers, 128KiB total, depth16 and 4096-byte strings.
Each case requires the SHA-256 of its exact complete diagnostic manifest. The
separate closed manifest schema requires all source/compiler/binary/profile/controller
identities, original root and controller identity, exact argv and empty environment.
A case with missing provenance cannot be recorded as measured success. It contains
no public constructor for a trusted admission token.

## Execution gates

Q0 independently reviews the exact helper, profile, positive controls, barriers,
bounds and cleanup protocol before execution. All harness actors, inherited handles
and channels are accounted for. No inherited credentials or provider configuration
are admitted. Build and runtime execution are offline and invoke no vendor CLI.

Q1 measures the original-launch fixed helper under a separately identified
no-fork/no-filesystem-effect profile. Workspace writes permitted by an earlier
backend profile cannot establish absence. Denied fork, spawn, filesystem,
network, delegation and parent signalling claims each require a functioning
positive control or remain explicitly unqualified. No read-confinement claim
follows from an isolated HOME or a missing credential file.

Q2 measures actual stdout/stderr capture and the existing original owner/storage
writer through finite barriers. Exit/reap, both EOFs, drain/join, input, raw extents,
final watermark, channels, writer resources and mutation reconciliation are
independent facts. Known capture loss blocks replacement. A successful commit or
ACK is not complete writer closure. Lost ACK/possibly committed commands stay
unknown and are never replayed. Owner death cannot release a live writer's claim.
Finite barriers and process kills do not qualify fsync failure, ENOSPC, power loss
or an indefinitely stalled kernel operation.

Q3 checks the same enforced-absence profile on original and successor execution.
A later receipt-backed idempotent-effect profile is separate; arbitrary edits
cannot be labelled repeat-safe by checking a diff. Python composition must retain
its own exact identity, authority/configuration/budget checks, issuance fences,
whole-job closure, resource handoff and one-use delivery. Physical helper closure
cannot substitute for a Python dispatcher or session/message/worktree journal.
Each required and incidental prerequisite must have an independent withheld-fact
rejection case. Original attempts remain charged and unknown usage is not zero.

## Cleanup and bounds

The controller retains original wait/control authority. A guardian, if used,
is enrolled before effects and has no uncontrolled old-domain mutation authority.
It stays alive to observe settlement and is itself joined/reaped before fixture
teardown. No PID-only discovery, group-only proof or retroactive descendant adoption.
A negative probe cannot start without an audited bounded cleanup domain. General
escaped descendants remain unsupported unless complete closure is enforced.

Per-profile bounds enumerate processes, threads, FDs, output, capture, FIFO/IPC,
read/hash work, pending commands, step size, wall deadlines and completion reserve.
The retained fixture budgets are not production execution/grace/stop defaults.
Unknown cleanup retains the root and evidence; no unconditional deletion on Drop.
Cleanup permission requires every owned actor and writer positively settled,
channels/readers reconciled and root ownership available.

## Mac handoff

The final receipt must separately list supported Python steering operations,
refused poststart profiles, measured helper obligations and unresolved production
obligations. Existing queueing and prestart withdrawal do not imply real poststart
replacement support. Mac consumes public ctl only and must not read these private
artifacts or persistence to invent a fallback. Capability publication and fixture
vendoring require the fully qualified accepting route; implementation evidence
alone does not unblock that route. Rust adoption and runtime/backend qualification
remain separate gates. No provider call, push, release or installation is authorized
by this document.
