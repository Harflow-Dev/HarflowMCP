# E2 lifecycle conformance: the generic pinned-target adapter and the fake-backend seam

Status: the adapter exists (`conformance/lifecycle`). **The owner decided O1 on 2026-11-12: option A plus 5**
(a separately built fake `claude` first on the case `PATH`, launched only at explicit `--freedom unrestricted`; no
engine selector, allow-list or evidence file), and the seam is built: `--seam path-fake`, the scripted fake in
`conformance/claude_fake` and its recording bundle. Open questions 2, 4, 5 and 6 below remain. Nothing in this
document changes a contract, the engine or Python behaviour.

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
- **No-spawn evidence needs an observable agent.** `refusals` checks stable codes and an empty public task list,
  but "the refused request started nothing" needs independent evidence of every descendant. A `ps` snapshot diff
  filtered on command-line markers cannot give it: it misses a detached process whose command names neither
  marker (for example `/bin/sleep 300`, self-checked), and macOS offers no unprivileged descendant tracking
  (kqueue `NOTE_TRACK` fails with `EOPNOTSUPP`, measured). Without a fake the harness can observe, the case
  records the diff as a weak hint, keeps the checks that did pass as partial evidence, and is **blocked**: the
  pass is withheld. With the Python injection seam the evidence is the observer's registrations, which prove
  only that no *agent (fake)* started. Cleanup is recorded per case.
- **Platform.** The observer needs Darwin kqueue and `LOCAL_PEERPID`. Lifecycle conformance is **unavailable on
  Linux**; the report says so and the observer tests are skipped there.

`python -m conformance.lifecycle --seam none` against the Python 0.0.11 source: `refusals` verifies its codes
and the empty task list and then reports `blocked` for lack of independent no-spawn evidence; the other five report
`blocked` for lack of a fake, so nothing passes and `complete` is false. `--seam python-injection` re-uses the S08
controlled-source profile (in-process backend replacement, a shim, extra environment) to prove the case logic:
all six pass on Python 0.0.11. That seam is a test of the **cases**, not of any pinned binary.

**The first validated run used the S08 observer**, which relies on participants registered by the Python injection. The
`OsObserver` (kernel peer PID, ancestry rendezvous, kqueue watches) is now integrated into the case logic by
`PathFakeSeam` and the six cases pass on it against Python 0.0.14. The seam interface the cases require, and how
`PathFakeSeam` provides it:

| Need | `PathFakeSeam` (the S08 injection seam provides the same through its own profile) |
| --- | --- |
| `launch` / `resume` returning the task, the fake and the owner (the fake's registered parent) | the engine's public `run`/`resume` at `--freedom unrestricted`; the fake is the fresh `fake` registration whose ledger names the task's session and session flag; the owner is the first observed chain hop, which must be the fake's parent with an armed exit watch |
| `exited(participant)`, `alive(participant)` | `OsObserver.wait_exited/exited`, keyed by PID and start time |
| `kill_owner(launch)` | a control file named by the fake's observer token; the fake calls `participant.kill_parent` and logs the result |
| `raw_evidence(launch)` from the fake's own ledger | the fake's ledger, re-checked digest by digest; untrusted corroboration, never the source of an exit code |
| cleanup that proves every participant is gone | gates and a stop file released, then every armed `(pid, lstart)` observed exited, the observer not failed, every engine command drained |
| `fakes_spawned`, `watch_spawns` (agents only) | observer registrations with role `fake`; ledgers of `kind: agent` (refused-argument ledgers are reported separately) |

## The open item: how the engine runs a fake agent

The tested binary must be the release binary and nothing may widen its authority, so the fake cannot be
injected into the engine. Options considered:

| Option | Verdict |
| --- | --- |
| **A. A separately built fake CLI installed first on `PATH` of the isolated case, reached through the engine's stock adapter path** | candidate; admissible only with a D6 option below, so not yet proposed for building |
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
  observer, whose socket path and nonce it reads from `fake.json` beside the installed fake (never from the environment).
- Spike (not committed): on Python 0.0.11 a trivial executable named `claude` on `PATH` was accepted by the
  stock Claude backend with only a `--version` probe, was started with the real argv
  (`-p --output-format stream-json --input-format stream-json --session-id ... --permission-mode ...`), and the
  task could be listed and cancelled. No selector or environment key was involved.
- For Rust this pulls the minimal Claude adapter and stream parser forward from E3 into E2 (as the plan's
  entry item already says), and the fake must satisfy whatever probes that adapter performs. It could also
  make `refusals` stronger with a refusal the stock adapter derives from the fake's declared capabilities
  (an option Claude declares unsupported). `--max-turns` is **not** such an example: Claude supports it, so it
  is not a refusal on that backend.

- The owner is defined as the fake's registered parent, which matches the owner-runtime table (the original
  owner is the agent's parent and sole waiter). If the Rust process layout interposes a helper between owner
  and agent, the rule "nearest ancestor that outlives the initial CLI" must be agreed.

### Conflict with D6: the fake's binary digest

D6 binds tool-level permission evidence to the binary digest, version, platform, probe revision and exact
grants, and a differing digest refuses **before effects**. A fake `claude` always has a different digest from the
qualified real one, so wherever the engine requires that evidence, it refuses the fake whatever `PATH` says.
Evidence gates the `os_enforced` and `tool_level` modes; it is not obviously required for an explicit
`unrestricted` request. Options:

| Option | Effect |
| --- | --- |
| 1. Allow-list the fake's digest in the engine | widens authority: whoever controls that list controls what is admitted, and the tested binary then has test-only admission logic |
| 2. Let the case supply an evidence file the engine trusts (in the state root or the environment) | widens authority: a writable evidence input is exactly what an agent could forge |
| 3. Qualify the fake through the engine's own unpaid qualification path inside the isolated root | the evidence says `passed` for a program that does not enforce what it is qualified for, so it is untrustworthy; if the probes would reject the fake, the cases cannot launch |
| 4. Defer the fake-based cases until the real Claude adapter and D1 qualification exist (E3) | no authority change, but it needs an E2 exit change and an E2-to-E3 order change, and E3 then has no runner for these cases (the real Claude is paid and uncontrolled), so they would have no home |
| **5. Run the fake at explicit `--freedom unrestricted`** | no selector, allow-list or evidence file; recommended, see below |

**Recommendation: option 5.** The evidence requirement gates `os_enforced` and `tool_level`;
`owner-runtime-v1.md` bans *falling back* to unrestricted when evidence is missing, not *requesting* it
explicitly. A lifecycle case that asks for `unrestricted` therefore needs neither evidence nor any change to the
tested binary. It also gives a positive test of the gate: the same fake launched at `read_only` or
`write_in_repo` must be **refused before spawn** from E3 on (the engine cannot match the fake's digest), which a
conformance case can assert.

Costs to state plainly:

- Any program named `claude` on `PATH` launches at `unrestricted` without a digest match, exactly as Python
  does today. That is the existing reality for explicit `unrestricted` requests, but the report must not hide it:
  the task's `enforcement` block has to disclose that no permission evidence was bound.
- `permission-mode-report-v1` has **no representation** for an unrestricted launch yet (its modes are
  `os_enforced` and `tool_level`); one has to be added before the report can say it truthfully.
- The cases then exercise lifecycle at `unrestricted`, not at the freedoms users normally choose; the gate test
  above is what keeps the other freedoms honest.
- **This needs the owner.** D6 literally says "0.0.11 freedom levels on stock Claude" are tool-level policy;
  whether an explicit `unrestricted` request is outside that sentence is an owner reading, not mine to assume.

**Decided (2026-11-12): option 5.** Option 4 remains the fallback if the owner reads D6 so that `unrestricted` also
needs evidence.

### What is built and what is not

Built: the target and environment checks, the OS observer and rendezvous, the participant client, the case logic behind
a seam interface, `NoSeam`, the Python validation seam, the runner and the self-checks, and (O1) `PathFakeSeam` with
the scripted fake `claude` (`conformance/claude_fake`) and its scrubbed recording bundle. The fake is installed per case
with its bundle, hashed (files, launcher and interpreter) and re-checked before every launch; `claude` must resolve to
the installed launcher through the case `PATH` before every engine call. That is a guard, not a sandbox: an engine
that resolves `claude` through the case `PATH` can only reach the fake, while one that resolves it another way (an
absolute path, its own `PATH` handling) is detected only after launch, by the missing registration, so a real CLI
could already have run. The Rust adapter must resolve `claude` only through the `PATH` it is given. It registers with the observer, writes
an untrusted ledger, ends its own recorded parent on request (`participant.kill_parent`) and exits by itself at a
lifetime bound. The cases are proved against Python 0.0.14 on the OS observer: all six pass on that seam with no
extra environment key. The recorded behaviour is protocol shape only; SIGTERM behaviour and refused-argument wording
are scripted and unmeasured, and `escape` is a fixture for survivor reporting, not evidence that reporting works.
**Not built:** the Rust Claude adapter and stream parser that will consume it (Engine repository), the survivor oracle
(E2-D) and any engine-side discovery change. Until a pinned Rust binary exists, a `path-fake` run against a source
target is never acceptance evidence.

## Open questions (recorded for the owner)

1. *Answered (2026-11-12).* The D6 conflict is resolved by option 5, explicit `unrestricted`, and the seam of option A
   is built. The disclosure costs above still apply: the task's `enforcement` block must say no permission evidence was
   bound, and `permission-mode-report-v1` needs an unrestricted representation before a report can say it.
2. Whether Rust's Claude adapter probes beyond the version (capability and feature detection) and how the
   fake answers them without becoming a copy of the real CLI.
3. *Answered.* The fake lives in this repository (`conformance/claude_fake`, with its recording bundle and `SHA256SUMS`)
   and is pinned by hash. A consumer (the Rust conformance runner, E2-C and E2-D) copies the directory, hashes it, and puts
   `bin/claude` first on the case `PATH`; no build or test here depends on a sibling checkout.
4. The owner-identification rule above for non-trivial process layouts. The seam implements the rule as: the observer's
   first chain hop must be the fake's parent and have its exit watch armed; a broken or truncated chain is recorded and
   accepted only beyond that first hop.
5. Darwin only: the observer uses `LOCAL_PEERPID` and kqueue; Linux needs its own mechanism before Web.
6. Survivor cases: #118's criteria ask for a bounded timeout while D12 has none (see
   `C1-task-survivors.md`); a lifecycle `cancel` case that leaves a survivor cannot be specified until that is
   decided.
