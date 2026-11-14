# Conformance infrastructure (S07/S08/S09)

This standard-library runner provides isolated black-box observations. It does
not import engine implementation helpers as outcome oracles. The S07 entrypoint
checks only fresh version-info handshakes and read-only `state-inspect` against a
missing root. All engine operations outside that allowlist are refused there.

S07 is a partial C0-MC-04 foundation. Task lifecycle, authority, messaging and
approvals belong to S08; workflows, resources and statistics acceptance belong to
S09. S07 standalone fake execution is **not engine-to-fake integration**, real
harness qualification, or proof of task execution. The separate S08 controlled
profile below adds an explicit synthetic backend and task scenarios.

## Run a pinned source boundary check

From this repository, use an existing interpreter; this command installs nothing:

```sh
S07_REPO="$PWD"
S07_ID="$(.venv/bin/python -c 'import uuid; print(uuid.uuid4().hex)')"
.venv/bin/python -m conformance \
  --source-repo "$S07_REPO" \
  --commit "$(git rev-parse v0.0.14^{commit})" \
  --python "$S07_REPO/.venv/bin/python" \
  --manifest "$S07_REPO/contracts/v1/manifest.json" \
  --manifest-sha256 c61bac6e1c3ca00eab4aab4bd1a8f063e98dd91c88365d4f5023127417f432cf \
  --output "/private/tmp/harflow-s07-$S07_ID" \
  --invocation-id "$S07_ID"
```

The output directory must be new. Keep output outside product repositories.
Artifacts remain available after both success and failure; the runner does not
recursively delete state, source checkouts or unknown paths. Use the independently
chosen invocation ID when calling `validate_report`, rather than accepting an ID
read from the report itself. A nonzero exit or `ok: false` is not acceptance.

The source target is a Git archive of the full exact commit, extracted under the
fresh output directory after rejecting unsafe archive entries. The public CLI
loads from its captured `src` directory under isolated Python; its loader must
resolve inside that snapshot. Source/runtime fingerprints are checked around
execution. No editable checkout is selected as the tested engine. The current infrastructure pins the complete 351-fixture 0.0.14 bundle (0.0.13 plus the `backend-models.v1` contract). The examples use this bundle, not the 0.0.12 manifest. Earlier S07/S08/S09
receipts retain their original source and manifest identities; this repin does not requalify
their scenarios, a managed runtime, or any consumer. The S08 and S09 profiles still snapshot the older 0.0.7 sources
(manifest `383ecf7a…`), which `verify_manifest` no longer accepts since the pinned digest moved
with 0.0.10; rerunning them needs a per-source digest and is not part of this repin.

The sixteen `ctl-backend-models-*.json` specimens exercise the real `backend-models` command with an injected clock,
a fake subprocess runner and no state root. `validate_backend_models` and `validate_backend_models_error` check the document shape independently of the
engine: the closed state, reason and source vocabularies, the `user` scope, the advisory flag, null harness version,
RFC 3339 timestamps, the row and byte bounds, unique ids and the `effort_levels` null-versus-empty distinction. The
missing-capability handshake requires `backend-models.v1`, and `COMMAND_CAPABILITIES` maps the command to it.

The nine `ctl-task-cancel-*.json` specimens exercise actual ctl serialization against isolated
qualified records/receipts, with controlled clocks, process probes and explicitly recorded fault
seams. The independent oracle checks the exact observation shape, ordering, coverage, row/byte
limits and minimal fallback. The missing-capability handshake requires `task-cancel-cascade.v1`.
Case provenance records qualification limits; S07's target smoke operations remain read-only.
Companion-app adoption must independently vendor the identical complete bundle/digest and require
the capability on a fresh handshake.

## Explicit binary target

A future engine can use `--engine-bin /absolute/path` with a caller-pinned
`--engine-sha256`. `HARFLOW_ENGINE_BIN` supplies the same explicit binary choice;
it still requires the digest. Binary and source options are mutually exclusive.
There is no engine search on PATH. Hashes identify observed bytes; they do not
establish publisher authenticity, an immutable OS or real harness qualification.
Only stub binary-adapter self-checks are part of S07; Rust is not qualified.

## Evidence and cleanup

- Every ordinary ctl operation gets its own fresh handshake. The response is then
  validated independently for product/format/integer wire version/release line,
  root, platform, contract and required capability. Unknown optional fields,
  event kinds and capabilities stay additive. Nested normalized events validate
  independently, preserving historical writer provenance.
- The fixture manifest is checked against the independently pinned digest above,
  then every file is hashed. A self-consistent substituted manifest is insufficient.
  The separate MCP schema inventory is not treated as ctl documents.
- HOME, XDG paths, temporary files, workspace and state root live under a fresh
  case. The child environment is constructed explicitly without inherited caller
  identities, provider credentials or Python paths. This is test isolation, not an
  OS filesystem/network sandbox.
- The parent records real process creation, argv, nonce, environment identity,
  capture bytes/hashes, exit and cleanup separately from the fake's own ledger.
  Fake observations alone never certify a spawn or outcome. Captures are limited
  while draining both streams, with bounded timeout and teardown.
- Group signals finish before the owned leader is reaped, avoiding blind signals
  to a saved, potentially recycled PGID. Uncertain cleanup fails the run and keeps
  its evidence. This does not prove containment of descendants that escape their
  original process group; detached engine owners are unimplemented here.
- Reports are published atomically without replacing existing files, and validated
  against invocation context, process ledgers and capture hashes. These checks
  detect missing/inconsistent evidence; they are not tamper-proof attestations
  against another process with the same filesystem permissions.
- Unknown outcomes never authorize automatic replay. No retry loop dispatches
  engine mutations. Read-only missing-root checks report observed final absence;
  they do not trace every transient filesystem access or prove store ownership.

## Infrastructure self-checks

```sh
.venv/bin/python -m pytest conformance/tests -q
```

These tests live outside the engine unit-test tree and use independent contract
expectations. They cover both successful infrastructure and deliberate refusal,
identity corruption, malformed data, capture bounds and cleanup faults. macOS
process-group/OS inspection may require access beyond a filesystem sandbox; a
blocked observation is failed or unknown, never silently passed. No provider or
installed agent is needed. See the S07 receipt for actual results and limits.

## S08 controlled task and authority profile

S08 is Darwin-only and requires independent observer/client preflight before any
engine tasks. Inspect the test result before passing its captured log and SHA-256
to the scenario runner; the runner binds those bytes, but does not interpret them
as proof that tests passed. Keep the caller-selected invocation ID outside the
report. Example, after observing a passing preflight:

```sh
.venv/bin/python -m pytest conformance/tests_s08 -q > /private/tmp/s08-preflight.log
# Inspect the exit status and complete log before continuing.
S08_ID="$(.venv/bin/python -c 'import uuid; print(uuid.uuid4().hex)')"
S08_GATE="$(shasum -a 256 /private/tmp/s08-preflight.log | cut -d ' ' -f 1)"
.venv/bin/python -m conformance.s08.scenarios \
  --invocation-id "$S08_ID" --repo "$PWD" \
  --commit 95b696ef491a49007434d36bf4ba9eed8c1b5b8a \
  --python "$PWD/.venv/bin/python" \
  --observer-preflight /private/tmp/s08-preflight.log \
  --observer-preflight-sha256 "$S08_GATE"
```

Each case archives that exact source commit, verifies the canonical manifest,
and creates disposable HOME/XDG/tmp/state and Git workspace paths. An isolated
shim replaces only the backend registry/default and propagates the same loader
through fresh supervisors. The actual engine forks and supervises its synthetic
child. This injection is recorded; it does not qualify stock backend loading,
lazy entrypoint purity, a real agent/provider, OS confinement or engine 0.0.8.

The external observer records kernel peer PID, ancestry and kqueue exit watches
before acknowledging participants. Known participants terminate themselves over
their existing control sockets; no saved non-child PID is signalled. This covers
registered ancestry, not escaped or unknown descendants. Synthetic I/O is marked
untrusted and corroborated with public task/prompt/event state, observed process
exit and fixed sentinel effects. Shared controller and overlay hashes are checked
before and after operations. Runtime closure limits from S07 remain.

The nine cases cover launch refusals, completed/nonzero outcomes, detached owners
and overlapping resume exclusion, cancellation, exact-action approvals, managed
actor refusal, queued messaging/cancellation/explicit consumption, and owner loss
without fabricated exit/drain evidence. Overlapping resume is not a simultaneous
race test. PID reuse, reboot, disk-full, full MCP restart and uncertain-control
fault families remain unsupported; workflows/resources/statistics belong to S09.

Artifacts are retained under a new `/private/tmp/h8-<ID-prefix>` directory; an
existing prefix refuses without replacement. Context is written before tasks;
the S08 report is updated after each case and stops at the first failure. Unlike
the S07 report publication API, these progress writes are not atomic publication.
Inspect the complete case set, captured operations, observer records and each
`cleanup.json`, not just the aggregate boolean. Temporary artifacts are not a
durable evidence store. Receipt acceptance and full C0 qualification remain
separate from a passing controlled-profile run.

## S09 workflow, resource and statistics profile

S09 extends the same controlled source approach. Verify the complete self-check
result before supplying its log and digest; the preflight argument binds external
bytes and does not itself attest that tests passed:

```sh
.venv/bin/python -m pytest conformance/tests conformance/tests_s08 conformance/tests_s09 -q \
  > /private/tmp/s09-preflight.log
# Inspect the exit status and complete log before continuing.
S09_ID="$(.venv/bin/python -c 'import uuid; print(uuid.uuid4().hex)')"
S09_GATE="$(shasum -a 256 /private/tmp/s09-preflight.log | cut -d ' ' -f 1)"
.venv/bin/python -m conformance.s09.scenarios \
  --invocation-id "$S09_ID" --repo "$PWD" \
  --commit b0185be855fd13acc94cb2d2008400d285d15df2 \
  --python "$PWD/.venv/bin/python" \
  --observer-preflight /private/tmp/s09-preflight.log \
  --observer-preflight-sha256 "$S09_GATE"
```

Output lives under a fresh `/private/tmp/h9-<ID-prefix>` with one isolated profile
per case. Existing prefixes refuse. The runner emits case progress and the final
report path, retains context/captures on failure, and stops at the first failed
case. Progress report writes are not atomic publication. `--case NAME` selects a
subset explicitly; a subset passing does not establish the complete matrix.

The observer additionally qualifies workflow owners and their supervised task
children. A bounded private CLI/owner rendezvous keeps the real CLI parent alive
until owner registration has armed its kernel identity/exit watch. An audit hook
observes exact run/root spawn intents; it does not replace engine spawning.
Parent acknowledgments bind kernel peer PID, parent, nonce and operation identity
and remain in per-command ledgers. Nested workflow runs share the registered root
owner; their separate identities come from qualified public run/attempt links.
Autonomous owner replacement during teardown remains unsupported and fail-closed.

Cases cover core definition operations, sequential/parallel/nested execution,
exact input decisions, explicit scheduler recovery and cancellation, whole-task
FIFO/owner uncertainty, scratch cleanup guards, disabled phase diagnostics, and
retained-history statistics. Orchestrator input proof uses issued checkpoint and
delivery receipts: public task prompts can be display labels, and a session can
span several control turns. Workers and managed child CLI requests retain their
own public/OS/capture evidence. Synthetic reported cost/model fields test
provenance and null semantics; they are not real provider charges.

Branch/fetch/worktree mutation options and alternate `run --workflow` routing are
excluded. The worktree case permits only the public read-only refusal probe;
`slot` permits only `status`, never enable/service operations. All cleanup is
limited to fresh owned test state and known registered participants. No owner
state, real provider, installation or registration is used.

The profile selects product 0.0.7 through an explicit source archive and existing
Python 3.11.15, with the S07/S08 runtime-closure limits. Stock backend loading,
lazy entrypoint purity, OS confinement, generic binary-target workflow execution,
Rust/platform/client qualification and full conformance remain unproven. See the
[S09 coverage ledger](../docs/migration-receipts/S09-coverage.md) for required
missing families and acceptance boundaries. S09 contributes evidence; it does not
approve C0 or authorize the next migration session.

## E1 read scenarios

`conformance/e1` checks the bounded ctl reads from outside the engine: `list`, `status`,
`status-batch`, `task-list-page` (including successor discovery), `task-events`, `task-prompt`,
`task-artifacts` (including its `data.reason` cursor errors), `recovery-activity`, `interaction-events` and
`statistics-page`, the `continuation_stop` diagnostic on status, brief and batch (`task-continuation-stop.v1`) and
pending launch discovery (`task-pending-page`, `task-pending-status`, `task-pending-launch.v1`), plus the caller and
scoped-read refusals around them. Case groups live in `cases.py`, `cases_stop.py`, `cases_pending.py`,
`cases_pending_gate.py` and `cases_artifact_reasons.py`. It is a separate entry point from S07 and imports no engine code.

- **Fixture port.** Cases create, damage and inspect state only through the named operations in
  `port.py` (`seed_world`, `add_task`, `add_live_caller`, `damage_task`, `append_event`,
  `append_partial_line`, `truncate_log`, `remove_log`, `tamper_prompt`, `write_artifacts`,
  `authoritative_digest`, `stop_chain`, `add_launch`, `add_invalid_launch_name`, and root helpers). No case writes under the state root and no store layout is
  part of the interface. `PythonLayoutAdapter` is the only code that knows Python 0.0.13's files and
  writes the documented private formats as literal bytes with the standard library. An external adapter
  is selected with `--fixture-adapter PATH --fixture-adapter-sha256 DIGEST` (invoked `ADAPTER OP --root ROOT`,
  JSON arguments on stdin, JSON result on stdout) and is never reachable from the public engine binary.
- **Complete inputs.** Every task field and every artifact item the cases assert (`world.task`, `world.artifact`) is an
  explicit input the adapter reads; an adapter has no defaults, and a reply to `add_live_caller` must carry an explicit
  boolean `supported` (an empty or non-boolean answer fails the scenario).
- **Oracle independence.** Expected values in `cases.py` are literals derived from `world.py` and
  `docs/contracts.md`. Nothing shares a codec with an engine, cursors are opaque, and error wording is
  never pinned (codes and identities only).
- **Tags.** A check tagged `[contract]` follows the published text. Every other check is `[baseline]`
  (the default tag): it pins observed 0.0.13 behaviour the contracts do not spell out, so an engine that
  differs must record a contract delta. `[baseline, pending U2]`, `[baseline, U4]`, `[pending U10]` and
  `[proposed]` mark owner decisions still open (see `docs/decisions/C1-caller-and-scoped-reads.md`).
- **Caller rules.** `agent_caller` is produced by registering the test process itself as a live task
  through the port, so the engine, as its descendant, is detected by ancestry. `scope.identity_verdicts`
  covers the alive, dead, undecidable, legacy and terminal-record identity states; environment markers
  (including empty values) and unverifiable task history are covered separately. Workflow-managed callers
  need workflow run documents and are listed for E6 in C1 section 6.1.
- **Read-only.** After each case the digest of all authoritative state must equal the seed's. The digest
  must be non-empty and must differ from an empty store's, so an adapter that hashes nothing fails.
- **Skips gate acceptance.** Only `legacy_*` identity states may be skipped by an adapter. `alive`, `reused`,
  `unknown` and a terminal observed state are required: unsupported means failure. The `recovery-activity`
  capture-loss branch is an `unverified` skip: the printed summary lists it and `complete` stays false unless
  it is excluded by name with `--exclude-unverified NAME`, which the report records. The exit status is zero
  only when `ok` and `complete`.
- **Delta profiles.** `--delta interim-6.1` and `--delta U10` replace the Python baseline expectations with
  the proposed alternatives of C1 section 6.1 and U10 (tagged `[proposed, ...]`) and are recorded in the
  report. `--delta pending-prefix-paging` replaces the one assertion that pins a Python 0.0.13 paging defect
  (`task-pending-page` resumes by bare task id, so with receipts `ab-c` and `ab` and `--limit 1` it skips
  `ab`) with the complete traversal. Python fails these profiles on purpose.
- **Launch receipts and markers.** `add_launch` writes a qualified receipt (a receipt with a `refusal` fails exactly
  that qualification) and replaces an existing name in place, so the set of names, and therefore the namespace
  revision and cursors, is unchanged. `stop_chain` writes the continuation-stop marker (`valid`, `invalid`,
  `malformed` or `oversize` attribution), a non-regular entry (`fault`) or removes it (`absent`). A `chain_root` on
  `add_task` follows Python (the predecessor's stored chain root, else the predecessor) unless given verbatim or as
  `null`. Cases that craft a cursor (position and artifact offset errors) decode its base64 JSON members; a cursor
  of another shape is an `unverified` skip.
- **Namespace revision.** The cases assert `namespace_revision` as SHA-256 over the receipt filenames
  (`<name>.launch.json`) in sorted order, each followed by a NUL byte; the empty namespace is the SHA-256 of nothing.
  Cursor tokens are otherwise opaque.
- **Not covered.** The 65,536-entry and 8 MiB namespace caps of the pending reads need a test-only seam in each
  engine (Python's own tests patch `MAX_ENTRIES`); no case seeds that many files. Resource-name truncation
  (256 bytes per name, more than 100 names) is unreachable: Python's `validate_names` limits a receipt to 16 names of
  at most 64 characters. Cursor expiry by name removal is not covered either: the port has no removal operation, so
  only addition expires a cursor.
- **Bounds.** Paging walks fail when the bound is exhausted. Re-issuing a read that reports preparation
  follows a harness bootstrap policy (40 attempts, 250 ms), not the contract.
- **Framework identity.** `conformance/*.py` and `conformance/e1/**` are hashed before the run, checked
  after it, and again when the report is validated.
- **Targets.** `--source-repo/--commit/--python` runs a Git archive of an exact commit;
  `--engine-bin/--engine-sha256` runs a caller-pinned binary. A subset (`--case`) is reported as incomplete.

```sh
.venv/bin/python -m conformance.e1.scenarios \
  --source-repo "$PWD" --commit "$(git rev-parse v0.0.14^{commit})" --python "$PWD/.venv/bin/python" \
  --manifest "$PWD/contracts/v1/manifest.json" \
  --manifest-sha256 c61bac6e1c3ca00eab4aab4bd1a8f063e98dd91c88365d4f5023127417f432cf \
  --output /path/to/new-empty-dir
.venv/bin/python -m pytest conformance/tests_e1 -q   # infrastructure self-checks
```

The self-checks include deviating-engine doubles (dropped fields, wrong status, missing or opened
authority refusals, ignored environment markers, unbound cursors, state rewritten by a read,
nonterminating paging, vacuous digests). For each, the same case must pass against the undoubled
engine, fail against the double for the stated reason, and the double must have logged its mutation.
Passing against Python 0.0.13 establishes the scenarios, not any other engine. The S08 and S09
profiles cannot be rerun on their old 0.0.7 manifest digest until the E2 port gives them a per-source
digest.

## Lifecycle conformance (generic pinned target)

`conformance/lifecycle` re-expresses the S08 cases `refusals`, `complete`, `nonzero`, `detached-resume`, `cancel` and
`owner-death` for a pinned binary (`--engine-bin/--engine-sha256`, re-hashed before every operation) with a closed
engine environment (no selector can be added), the product line carried in the target identity
(`--product-version`), public commands only, and OS-level observation (kernel peer PID, ancestry rendezvous, kqueue
watches on the participant and its ancestors) instead of hooks inside the engine. **Darwin only: lifecycle
conformance is unavailable on Linux**, and the observer self-checks are skipped there.

A case that needs a running agent on a seam without one is `blocked`, never passed. The report separates
`no_failures`, `all_passed`, `subset` and `complete`; the exit status is zero only when `complete`. A run is
`acceptance_eligible` only when the pinned binary equals a release digest and commit supplied independently
(`--release-sha256`, `--release-commit`) and the seam is eligible. `refusals` verifies its codes and the empty task
list, but "nothing was spawned" needs an observable agent, so without a fake it is `blocked` with its partial
evidence (a command-text process diff cannot see a detached `/bin/sleep`). `--seam none` therefore blocks all six; `--seam python-injection` re-uses the S08 profile only to prove the case logic on Python and is
never acceptance evidence (its extra environment keys are reported). The fake-backend seam and its conflict with
D6 are open design items: see `docs/decisions/E2-lifecycle-conformance-seam.md`.

```sh
.venv/bin/python -m conformance.lifecycle --source-repo "$PWD" --commit "$(git rev-parse v0.0.14^{commit})" \
  --python "$PWD/.venv/bin/python" --manifest contracts/v1/manifest.json \
  --manifest-sha256 c61bac6e1c3ca00eab4aab4bd1a8f063e98dd91c88365d4f5023127417f432cf \
  --product-version 0.0.14 --seam none --output /path/to/new-dir
.venv/bin/python -m pytest conformance/tests_lifecycle -q   # self-checks
```
