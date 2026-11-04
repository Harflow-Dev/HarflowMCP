# Conformance infrastructure (S07)

This standard-library runner provides isolated black-box observations. It does
not import engine implementation helpers as outcome oracles. It currently checks
only fresh version-info handshakes and read-only `state-inspect` against a missing
root. All engine operations outside that allowlist are refused.

S07 is a partial C0-MC-04 foundation. Task lifecycle, authority, messaging and
approvals belong to S08; workflows, resources and statistics acceptance belong to
S09. Standalone fake execution is **not engine-to-fake integration**, real harness
qualification, or proof of task execution. The fake's minimal text/result protocol
still needs the explicitly selected backend protocol adapter in S08 before task
launches can be tested.

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
