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
  --commit 0f10ad30e28e7da1873685cebe70aef687881723 \
  --python "$S07_REPO/.venv/bin/python" \
  --manifest "$S07_REPO/contracts/v1/manifest.json" \
  --manifest-sha256 383ecf7a9b234a1ece18eb15585398f08a77205c02a43cb2a928a2819f76fee0 \
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
execution. No editable checkout is selected as the tested engine. The initial
S07 target is source version 0.0.7 on Python 3.11.15, not S02's managed Python
3.13.16 and not the intended Electron 0.0.8 development runtime.

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
