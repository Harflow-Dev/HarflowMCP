# E2 lifecycle conformance: the generic pinned-target adapter and the fake-backend seam

Status: the adapter exists (`conformance/lifecycle`); the **fake-backend seam is an open design item** and
only a proposal is written here. Nothing in this document changes a contract, the engine or Python behaviour.

## What the adapter is

The S08 lifecycle cases `refusals`, `complete`, `nonzero`, `detached-resume`, `cancel` and `owner-death`
re-expressed so they need nothing from Python:

- **Pinned target.** The engine is a caller-pinned binary (`--engine-bin` plus `--engine-sha256`,
  re-hashed before every operation) or, for Python validation only, an exact-commit source launcher. A source
  launcher is never the release binary and its runs are never `acceptance_eligible`.
- **Pinned, not "release".** Identity is `pinned_binary`. A run is `acceptance_eligible` only when the pinned
  binary's digest equals a release digest **and** commit supplied independently of the run (`--release-sha256`,
  `--release-commit`), the seam itself is eligible, and every case passed. The product line the engine must speak
  (`--product-version`) is part of the target identity, not a hidden pin.
- **No authority-widening selector.** The engine environment is built from a closed allowlist (`PATH`, `HOME`,
  XDG directories, `TMPDIR`, locale, `PYTHONDONTWRITEBYTECODE`, `HARFLOW_STATE_ROOT`). Any other key (a
  backend selector, an injection hook, an interpreter path, a task or depth marker) is refused before a
  process starts. A seam that needs more must declare the keys; its runs are then ineligible for acceptance.
  (`run_process` also adds a `CONFORMANCE_NONCE` variable to the child for the harness's own ledger; engines
  ignore it and it selects nothing.)
- **Public commands only.** `run`, `resume`, `status`, `cancel`, `task-events`, `task-prompt`, `list`, with a
  fresh handshake before each. No state is read or written under the root by the cases.
- **Non-Python, OS-level observers.** A fake participant registers with a small Unix-socket observer
  (`observer.py`). The observer takes the participant PID from the kernel (`LOCAL_PEERPID`), walks the
  ancestry with `ps`, and arms kqueue exit watches on the participant **and every non-harness ancestor
  before it acknowledges** (the ancestry rendezvous). The engine's detached owner is therefore observed
  without any hook in the engine and without the harness holding a saved PID of it. Evidence is a JSONL
  ledger of registrations and exits.
- **Owner death without signalling a saved PID.** The participant ends its own recorded parent
  (`participant.kill_parent`), only if `getppid()` still equals the parent the observer recorded; a
  forged or changed parent signals nothing (self-checked).
- **Blocked is not passed.** A case that needs an agent on a seam without one is reported `blocked`. The report
  says `no_failures` (nothing failed), `all_passed` (nothing failed or blocked), `subset` (a `--case` selection)
  and `complete` (all six passed); the exit status is zero only when `complete`. A passing subset is never
  acceptance.
- **Independent refusal evidence.** `refusals` needs more than "the engine said no": it takes a `ps` snapshot
  diff for processes naming the isolated case or the pinned engine, and withholds the pass when the process
  table cannot be read. This sees only processes still alive at the end; it complements the empty public task
  list and does not claim to see a process that started and exited. Cleanup is recorded per case.
- **Platform.** The observer needs Darwin kqueue and `LOCAL_PEERPID`. Lifecycle conformance is **unavailable on
  Linux**; the report says so and the observer tests are skipped there.

`python -m conformance.lifecycle --seam none` against the Python 0.0.11 source: `refusals` passes (no
fake needed: unknown backend, resume and cancel of an unknown task, an unknown option, and no task record,
spawn or evidence afterwards); the other five report `blocked`. `--seam python-injection` re-uses the S08
controlled-source profile (in-process backend replacement, a shim, extra environment) to prove the case
logic: all six pass on Python 0.0.11. That seam is a test of the **cases**, not of any release binary.

## The open item: how the engine runs a fake agent

The tested binary must be the release binary and nothing may widen its authority, so the fake cannot be
injected into the engine. Options considered:

| Option | Verdict |
| --- | --- |
| **A. A separately built fake CLI installed first on `PATH` of the isolated case, reached through the engine's stock adapter path** | proposed |
| B. An engine environment variable or flag selecting a fixture backend | rejected: a selector that changes what the release binary does, and an authority question (the same selector could be set by an agent) |
| C. A test-only build or cargo feature with a built-in fake | rejected: the tested binary is not the release binary |
| D. Replaying recorded provider transcripts to a stubbed network endpoint | rejected for lifecycle: it exercises parsing, not process ownership, cancel or owner death, and needs egress control the plan does not have |

### Proposal: option A

- The fake is a standalone program, built and hash-pinned separately from the engine, installed as
  `<case>/bin/claude` with `PATH=<case>/bin:/usr/bin:/bin`. The engine finds it the way it finds the real CLI.
  `PATH` is how an operator legitimately selects the CLI; the engine's own permission, ownership and approval
  rules apply to it unchanged, so nothing is widened.
- Requirements on the fake: it exits when its observer socket or engine pipes reach EOF **and** has a mandatory
  hard lifetime bound (`participant.bound_lifetime`), so a failed harness cannot leave it running; and it must not
  outlive the case without the observer having recorded its exit.
- The engine's detached owner must run in **its own session or process group**: `run_process` signals the CLI's
  process group after the CLI exits, so an owner left in that group would be killed by the harness. This is a
  requirement on the Rust owner (it already detaches in Python) and a case assertion in `detached-resume`.
- It speaks the real Claude stream protocol the engine already parses: answers the version probe, then
  `init`, assistant and `result` lines on stdout for the single `--input-format stream-json` input frame, and
  exits with the scripted code. Behaviour (complete, nonzero exit 7, hold until a gate file, hold and
  survive EOF) is scripted in the prompt text, as S08 does, so no environment is needed. It registers with the
  observer, whose socket path and nonce it reads from a case-local file under `$HOME`.
- Spike (not committed): on Python 0.0.11 a trivial executable named `claude` on `PATH` was accepted by the
  stock Claude backend with only a `--version` probe, was started with the real argv
  (`-p --output-format stream-json --input-format stream-json --session-id ... --permission-mode ...`), and the
  task could be listed and cancelled. No selector or environment key was involved.
- For Rust this pulls the minimal Claude adapter and stream parser forward from E3 into E2 (as the plan's
  entry item already says), and the fake must satisfy whatever probes that adapter performs. It could also
  make `refusals` stronger with a refusal the stock adapter derives from the fake's declared capabilities
  (an option Claude declares unsupported). `--max-turns` is **not** such an example: Claude supports it, so it
  is not a refusal on that backend.

### Conflict with D6: the fake's binary digest

D6 binds tool-level permission evidence to the binary digest, version, platform, probe revision and exact
grants, and a differing digest refuses **before effects**. A fake `claude` always has a different digest from the
qualified real one, so under D6 the engine will refuse to launch it, whatever `PATH` says. No fake can be
admitted without widening something. Options:

| Option | Effect |
| --- | --- |
| 1. Allow-list the fake's digest in the engine | widens authority: whoever controls that list controls what is admitted, and the tested binary then has test-only admission logic |
| 2. Let the case supply an evidence file the engine trusts (in the state root or the environment) | widens authority: a writable evidence input is exactly what an agent could forge |
| 3. Qualify the fake through the engine's own unpaid qualification path inside the isolated root | the evidence says `passed` for a program that does not enforce what it is qualified for, so it is untrustworthy; if the probes would reject the fake, the cases cannot launch |
| 4. Defer the fake-based cases until the real Claude adapter and D1 qualification exist (E3) and treat the E2 evidence for them as an owner-accepted limit | no authority change; E2 exit has less evidence |

**Recommendation: option 4**, because every alternative either changes what the tested binary admits or lets a
writable input widen authority. This is the owner's decision. **No fake is built for E2 until it is made.**
- The owner is defined as the fake's registered parent, which matches the owner-runtime table (the original
  owner is the agent's parent and sole waiter). If the Rust process layout interposes a helper between owner
  and agent, the rule "nearest ancestor that outlives the initial CLI" must be agreed.

### What is built and what is not

Built, independent of the seam: the target and environment checks, the OS observer and rendezvous, the
participant client, the case logic behind a seam interface, `NoSeam`, the Python validation seam, the runner
and the self-checks. **Not built:** the fake CLI of option A, its `PATH` wiring, and any engine-side
discovery change. Until a seam is chosen and built, `complete`, `nonzero`, `detached-resume`, `cancel` and
`owner-death` cannot be run against a pinned binary and must be reported as blocked.

## Open questions (recorded for the owner)

1. Approve option A, or choose another seam, and decide the D6 conflict above (options 1 to 4).
2. Whether Rust's Claude adapter probes beyond the version (capability and feature detection) and how the
   fake answers them without becoming a copy of the real CLI.
3. Where the fake lives and how it is pinned (this repository, the Engine repository, or a shared
   artifact), given that builds must not depend on sibling checkouts.
4. The owner-identification rule above for non-trivial process layouts.
5. Darwin only: the observer uses `LOCAL_PEERPID` and kqueue; Linux needs its own mechanism before Web.
6. Survivor cases: #118's criteria ask for a bounded timeout while D12 has none (see
   `C1-task-survivors.md`); a lifecycle `cancel` case that leaves a survivor cannot be specified until that is
   decided.
