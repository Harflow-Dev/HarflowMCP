"""Drives the real agent CLIs' `build_resume_argv` output through each binary's own parser.

Opt-in, and deliberately *not* under `PB_INTEGRATION`, which means runs that spend money. These are
designed to spend nothing — see the caveats below for what that design does and does not guarantee —
but they depend on optional external binaries, and on those binaries' current flag and output shapes,
which is exactly what the default suite promises not to.

    PB_CLI_INTEGRATION=1 uv run pytest -m cli_integration

Everything the fake-runner tests in `test_backends.py` assert about *policy* is asserted there, purely
in-process, against argv this backend built. What can only be learned here is whether the argv still
means what it meant when it was measured — i.e. whether the real binary's parser still accepts the
shape `build_resume_argv` emits, and still rejects an evidently malformed one. This exists because the
bug it guards against (`codex exec resume` rejecting `-C`/`-s`, see `backends/codex.py`) shipped for
exactly this reason: `tests/test_integration.py` only ever starts a task, so a resume argv had never
been run past a real CLI's parser, on any of the four backends.

For each installed backend, in a throwaway git repo, with `stdin=DEVNULL`:

    argv = backend.build_resume_argv(PROMPT, session_id=BOGUS_SESSION_ID, freedom="read_only", ...)
    ok  = run(argv)                     # the real argv this backend builds
    bad = run(with_bogus_flag(argv))    # the same argv, plus one bogus flag in the option region

    assert not looks_like_a_parse_rejection(*ok)   # our real argv is accepted
    assert looks_like_a_parse_rejection(*bad)      # the detector still recognises THIS binary

`looks_like_a_parse_rejection` is one predicate applied uniformly to all four backends' output. It
lives here, not in `backends/`, and branches on no backend's name — it reads only exit code and
output text. The `bad` run is what keeps it honest: without it, a CLI that reworded or dropped its
parse-error message would make `ok` pass for the wrong reason (predicate too weak) rather than
failing loudly (predicate no longer fits this binary).

Two caveats, spelled out rather than left implicit in "this costs nothing":

* **A UUID-shaped bogus session id is deliberate, and load-bearing for codex specifically.** Measured
  (codex-cli 0.154.0): codex rejects an unknown *UUID* locally (`no rollout found for thread id …`,
  no model call), but treats an unknown *non-UUID* as a thread *name* and starts a fresh thread,
  which does reach the API. A non-UUID bogus id would silently turn this into a paid test for codex.
  The other three backends were measured (2026-09-16, see below) to reject the same UUID locally too,
  but that was not proven true by construction the way the codex case was — it is what the `run`
  timeout below exists to bound if it is ever wrong.
* **The timeout-then-kill is a backstop, not a proof of zero cost.** If some future version of a
  backend does not reject an unknown session id locally and instead forwards the turn, this test
  bounds how long that run is allowed to continue before being killed — it does not guarantee the
  request never reached the provider, only that it cannot run to completion here. A kill is
  therefore asserted as a *failure*, not quietly read as "no parse rejection". The child is spawned
  in its own session and the whole group is killed, because these binaries spawn the real agent as
  a grandchild; that is the most this can claim about cleanup, not that nothing survives.

Measured 2026-09-16 on this machine, one run each, against a bogus UUID session id — all locally
rejected well inside the timeout, none via a parse-rejection shape:

* claude (2.1.273): exit 1, `No conversation found with session ID: <uuid>` — 1.2s.
* codex (0.154.0): exit 1, `no rollout found for thread id <uuid>` — 0.4s.
* opencode (1.18.30): exit 1, `Error: Session not found` — 0.6s.
* vibe (2.25.1): exit 1, `Error: Session not found: <uuid>` — 0.7s.

And the control, same run — `BOGUS_FLAG` inserted where this backend writes its own options (see
`with_bogus_flag`), which for codex means inside `codex exec resume`'s own parser, the one whose
option handling this fix is about:

* claude: exit 1, `error: unknown option '--…'` — caught by the `unknown option` marker.
* codex: exit 2, `error: unexpected argument '--…' found` under `Usage: codex exec resume [OPTIONS]
  [SESSION_ID] [PROMPT]` — caught by exit code alone, and demonstrably from the subcommand parser.
* vibe: exit 2, `usage: vibe [-h] …` — caught by exit code and the `usage:` marker.
* opencode: exit 1, and — measured, not assumed — **no "unknown"/"unrecognized"/"usage:" wording at
  all**. Its yargs-based parser answers an unrecognised flag with the invoked subcommand's own
  synopsis (`opencode run [message..]`, then `Positionals:` / `Options:` sections) and exit 1, which
  is indistinguishable from `--help` except for the exit code. `--help` itself exits 0, so "both
  section headers present, plus a nonzero exit" is a fourth, opencode-shaped marker rather than
  folding opencode into the generic text markers.

"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
from pathlib import Path

import pytest

from polybridge import backends

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(
        not os.environ.get("PB_CLI_INTEGRATION"),
        reason="drives real agent CLIs; opt in with PB_CLI_INTEGRATION=1",
    ),
]

PROMPT = "say ok and stop"
# UUID-shaped: see the module docstring for why that spelling matters (codex specifically), and why
# it was measured, not assumed, for the other three.
BOGUS_SESSION_ID = "00000000-0000-4000-8000-000000000000"
BOGUS_FLAG = "--totally-bogus-flag-nothing-writes-xyz"
RUN_TIMEOUT_SECONDS = 30
# A SIGKILLed process group closes its pipes at once; this only stops a wedged reap from hanging
# the suite in place of the timeout it was meant to report.
KILL_GRACE_SECONDS = 5


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True, capture_output=True)
    return tmp_path


def run(argv: list[str], cwd: Path) -> tuple[int | None, str, str]:
    """Run `argv` to completion, or kill its whole process group at `RUN_TIMEOUT_SECONDS`.

    `exit_code=None` marks a kill, and every caller treats that as a failed probe rather than a
    pass — a CLI that did not answer locally is exactly the case the module docstring warns may
    have reached a provider.

    `start_new_session=True` plus `killpg`, rather than `subprocess.run`'s own timeout handling
    which kills the direct child only. At least codex does not run as a single process — its `node`
    launcher execs a vendored binary, seen as a separate pid in `ps` — so killing the leader alone
    can leave that grandchild running and holding the pipes open. This is the same reason
    `tasks.py` captures a pgid at spawn; opencode is the one documented to have no grandchild.
    """
    proc = subprocess.Popen(
        argv,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=RUN_TIMEOUT_SECONDS)
        return proc.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        try:
            # Bounded, because a SIGKILLed group should close its pipes immediately and a hang
            # here would replace a reported timeout with a silent one.
            stdout, stderr = proc.communicate(timeout=KILL_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            # Only reachable if something outside the killed group still holds a pipe open. The
            # leader is already dead, so closing our ends and reaping it is all that is left —
            # without it this branch returns leaving an open fd and a zombie behind.
            stdout, stderr = "", ""
            for pipe in (proc.stdout, proc.stderr):
                if pipe is not None:
                    with contextlib.suppress(OSError):
                        pipe.close()
            with contextlib.suppress(subprocess.TimeoutExpired):
                proc.wait(timeout=KILL_GRACE_SECONDS)
        return None, stdout, stderr


def looks_like_a_parse_rejection(exit_code: int | None, stdout: str, stderr: str) -> bool:
    """One predicate, applied uniformly, for "the CLI refused this argv as malformed" — as opposed
    to any other failure (an unknown session id, a network error, ...). See the module docstring for
    what each of the four backends was measured to do, and why the last branch exists for opencode.
    """
    combined = f"{stdout}\n{stderr}".lower()
    if exit_code == 2:
        return True
    if any(
        marker in combined
        for marker in (
            "unexpected argument",
            "unknown option",
            "unrecognized arguments",
            "unrecognised arguments",
            "usage:",
        )
    ):
        return True
    # opencode-shaped: no wording at all, just the invoked (sub)command's own synopsis and a
    # nonzero exit (measured — see module docstring). `--help` prints the identical two sections
    # but exits 0, so requiring a nonzero exit here does not turn a legitimate `--help` into a
    # false positive.
    return exit_code != 0 and "positionals:" in combined and "options:" in combined


def with_bogus_flag(argv: list[str]) -> list[str]:
    """Put `BOGUS_FLAG` where this backend puts its own options — before the `--` separator, or at
    the end for a backend that writes none (vibe, whose prompt rides on `--prompt=`).

    Deliberately not "right after the binary name", which on a subcommand CLI probes the *top-level*
    parser. The regression being guarded is one subcommand deeper: `codex exec resume` rejecting an
    option `codex exec` accepts. A control that never reaches that parser cannot show the predicate
    recognises its refusals.
    """
    cut = argv.index("--") if "--" in argv else len(argv)
    return [*argv[:cut], BOGUS_FLAG, *argv[cut:]]


@pytest.mark.parametrize("name", sorted(backends.BACKENDS))
def test_real_resume_argv_parses_and_a_bogus_flag_is_caught(name: str, repo: Path) -> None:
    backend = backends.get(name)
    if not backends.is_installed(backend):
        pytest.skip(f"`{backend.binary}` is not installed")

    argv = backend.build_resume_argv(
        PROMPT,
        repo=repo,
        freedom="read_only",
        session_id=BOGUS_SESSION_ID,
        model=None,
        max_turns=None,
        reasoning_effort=None,
    )

    ok_code, ok_out, ok_err = run(argv, repo)
    # A kill is not a pass. It means the CLI neither accepted nor refused locally within the
    # timeout, which is the one case the module docstring flags as possibly having reached a
    # provider — so it is reported, never shrugged off as "not a parse rejection".
    assert ok_code is not None, (
        f"{name} did not answer within {RUN_TIMEOUT_SECONDS}s and was killed, so this probe proves "
        f"nothing — and it may have forwarded the turn: stdout={ok_out!r} stderr={ok_err!r}"
    )
    assert not looks_like_a_parse_rejection(ok_code, ok_out, ok_err), (
        f"{name}'s real resume argv was rejected as malformed — the argv shape no longer matches "
        f"what this binary accepts: exit={ok_code} stdout={ok_out!r} stderr={ok_err!r}"
    )

    bad_code, bad_out, bad_err = run(with_bogus_flag(argv), repo)
    assert bad_code is not None, (
        f"control inconclusive: {name} was killed at {RUN_TIMEOUT_SECONDS}s instead of refusing a "
        f"bogus flag: stdout={bad_out!r} stderr={bad_err!r}"
    )
    assert looks_like_a_parse_rejection(bad_code, bad_out, bad_err), (
        f"control failed: {name} did not reject a bogus flag as malformed, so this predicate no "
        f"longer recognises this binary's rejection style: exit={bad_code} stdout={bad_out!r} "
        f"stderr={bad_err!r}"
    )


def test_the_pre_fix_codex_resume_shape_is_still_rejected_by_the_real_cli(repo: Path) -> None:
    """The regression this whole file exists for, asserted against the binary rather than against
    our own `assert_safe`: `codex exec resume <options>` — the shape that shipped — must still be
    refused by codex's own parser. Built by moving the `resume` token of the real argv, so it stays
    the argv this backend would produce in every other respect.
    """
    backend = backends.get("codex")
    if not backends.is_installed(backend):
        pytest.skip(f"`{backend.binary}` is not installed")

    argv = backend.build_resume_argv(
        PROMPT, repo=repo, freedom="read_only", session_id=BOGUS_SESSION_ID,
        model=None, max_turns=None, reasoning_effort=None,
    )
    separator = argv.index("--")
    assert argv[separator - 1] == "resume"
    pre_fix = [*argv[:2], "resume", *argv[2 : separator - 1], *argv[separator:]]

    code, out, err = run(pre_fix, repo)
    assert code is not None, f"codex was killed instead of refusing: stdout={out!r} stderr={err!r}"
    assert looks_like_a_parse_rejection(code, out, err), (
        f"codex accepted the pre-fix resume shape, so the fact this fix rests on no longer holds: "
        f"exit={code} stdout={out!r} stderr={err!r}"
    )
