"""Codex backend.

CLI facts established by capturing a real run, not assumption:

* `codex exec` **blocks forever reading stdin** unless stdin is closed. Spawning with
  `stdin=DEVNULL` is mandatory, not tidiness.
* An approval prompt would hang a headless run just as badly, so `approval_policy="never"` is pinned
  alongside the sandbox mode.
* The event stream is nothing like Claude's::

      {"type":"thread.started","thread_id":"019fadb8-…"}
      {"type":"turn.started"}
      {"type":"item.completed","item":{"type":"error","message":"…"}}
      {"type":"item.completed","item":{"type":"agent_message","text":"ok"}}
      {"type":"turn.completed","usage":{"input_tokens":27992,…}}

  The session id is `thread_id`. The final answer is an `agent_message` item. There is no terminal
  event carrying success or failure, and **no dollar cost** — only token counts.
* An `item.type == "error"` was observed in a *successful* run (a benign skills warning), so error
  items are notices, never proof of failure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import (
    Accumulator,
    Capabilities,
    Enforcement,
    Freedom,
    Status,
    UnsupportedCapability,
)

BINARY = "codex"

SANDBOX_MODES: dict[str, str] = {
    "read_only": "read-only",
    "write_in_repo": "workspace-write",
    "unrestricted": "danger-full-access",
}

# Without this a headless run can stop dead waiting for an approval nobody can give.
NEVER_ASK = ("-c", 'approval_policy="never"')

# `workspace-write` is *not* repo-only: Codex reports its own writable set as
# `[workdir, /tmp, $TMPDIR]`, so claiming confinement to the repository would overstate it.
WRITABLE_ROOTS: dict[str, tuple[str, ...]] = {
    "read_only": (),
    "write_in_repo": ("the working directory", "/tmp", "$TMPDIR"),
    "unrestricted": ("anywhere the user can write",),
}

_MODE_CAVEATS: dict[str, tuple[str, ...]] = {
    "read_only": ("the sandbox rejects writes outright",),
    "write_in_repo": (
        "writes are confined by the OS, but to the workspace *plus* temporary directories — not to "
        "the repository alone",
        "no per-command deny list, so the agent can freely commit inside the sandbox",
    ),
    "unrestricted": (
        "danger-full-access disables the sandbox: nothing is restricted, which is the one mode where "
        "codex is no safer than an unsandboxed agent",
    ),
}


class UnsafeInvocationError(RuntimeError):
    """An argv was assembled without this backend's required guarantees."""


class CodexBackend:
    name = "codex"
    binary = BINARY
    capabilities = Capabilities(
        # Codex mints its own thread id and reports it in the stream.
        chooses_session_id=False,
        supports_turn_cap=False,
        reports_cost_usd=False,
        os_sandbox=True,
        per_command_deny=False,
    )

    def build_start_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str | None,
        model: str | None,
        max_turns: int | None,
    ) -> list[str]:
        if session_id is not None:
            raise ValueError("codex mints its own session id; one cannot be supplied")
        self._reject_turn_cap(max_turns)
        argv = [BINARY, "exec", *self._options(repo, freedom, model)]
        # `--` then the prompt: last, and explicitly not parsed as an option however it looks.
        argv += ["--", self._check_prompt(prompt)]
        self.assert_safe(argv)
        return argv

    def build_resume_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str,
        model: str | None,
        max_turns: int | None,
    ) -> list[str]:
        if not session_id:
            raise ValueError("resuming codex needs the thread id its first run reported")
        self._reject_turn_cap(max_turns)
        argv = [BINARY, "exec", "resume", *self._options(repo, freedom, model)]
        # `codex exec resume [SESSION_ID] [PROMPT]` — both positional, after `--`.
        argv += ["--", session_id, self._check_prompt(prompt)]
        self.assert_safe(argv)
        return argv

    def _options(self, repo: Path, freedom: Freedom, model: str | None) -> list[str]:
        options = ["--json", "-C", str(repo), "-s", SANDBOX_MODES[freedom], *NEVER_ASK]
        if model:
            options += ["-m", model]
        return options

    def _reject_turn_cap(self, max_turns: int | None) -> None:
        if max_turns is not None:
            raise UnsupportedCapability(
                f"the codex CLI has no turn cap, so max_turns={max_turns} cannot be honoured; "
                "omit it rather than have it silently ignored"
            )

    @staticmethod
    def _check_prompt(prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        return prompt

    def assert_safe(self, argv: list[str]) -> None:
        if argv[:2] != [BINARY, "exec"]:
            raise UnsafeInvocationError(f"unrecognised codex argv layout: {argv!r}")

        # Only the option region is inspected. Everything after `--` is positional — prompt text
        # that happens to contain a flag name must not be able to satisfy a safety check.
        if "--" not in argv:
            raise UnsafeInvocationError(
                f"refusing to run codex without a `--` separator before the prompt, which stops "
                f"prompt text being parsed as options: {argv!r}"
            )
        options = argv[: argv.index("--")]

        if "--json" not in options:
            raise UnsafeInvocationError(f"refusing to run codex without --json: {argv!r}")

        # `-s` and `--sandbox` are the same option; a second occurrence of either would win.
        sandbox_flags = [flag for flag in options if flag in ("-s", "--sandbox")]
        if len(sandbox_flags) != 1:
            raise UnsafeInvocationError(f"expected exactly one sandbox flag: {argv!r}")
        mode = options[options.index(sandbox_flags[0]) + 1]
        if mode not in set(SANDBOX_MODES.values()):
            raise UnsafeInvocationError(f"unexpected sandbox mode {mode!r}")

        # A missing never-ask policy is a hang, not a warning. Checked as a `-c` pair, and as the
        # only approval_policy override, since a later one would take precedence.
        pairs = [
            options[i + 1] for i, flag in enumerate(options[:-1]) if flag == "-c" or flag == "--config"
        ]
        approvals = [value for value in pairs if value.startswith("approval_policy")]
        if approvals != [NEVER_ASK[1]]:
            raise UnsafeInvocationError(
                f"codex must be given exactly one {NEVER_ASK[1]} override, which prevents it "
                f"blocking on an approval prompt no one can answer; found {approvals!r}: {argv!r}"
            )
        if "--dangerously-bypass-approvals-and-sandbox" in argv:
            raise UnsafeInvocationError(
                "--dangerously-bypass-approvals-and-sandbox discards the sandbox that is this "
                "backend's main safety property"
            )

    def enforcement(self, freedom: Freedom) -> Enforcement:
        mode = SANDBOX_MODES[freedom]
        unrestricted = freedom == "unrestricted"
        return Enforcement(
            freedom=freedom,
            mechanism=f"codex sandbox: {mode}",
            # Imposed by the OS, not by the agent's own judgement — except when switched off.
            os_enforced=not unrestricted,
            writes_confined=not unrestricted,
            writable_roots=WRITABLE_ROOTS[freedom],
            # Codex has no per-command deny list at all, so neither claim can be made.
            commit_push_blocked=False,
            direct_commit_commands_denied=False,
            caveats=_MODE_CAVEATS[freedom],
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        event_type = event.get("type")

        if event_type == "thread.started":
            thread_id = event.get("thread_id")
            if acc.session_id is None and isinstance(thread_id, str) and thread_id:
                acc.session_id = thread_id

        elif event_type == "turn.completed":
            acc.num_turns = (acc.num_turns or 0) + 1
            usage = event.get("usage")
            if isinstance(usage, dict):
                acc.usage = usage
            acc.terminal = event

        elif event_type == "item.completed":
            item = event.get("item")
            if not isinstance(item, dict):
                return
            item_type = item.get("type")
            if item_type == "agent_message":
                text = item.get("text")
                if isinstance(text, str):
                    acc.summary = text
                    acc.saw_final_message = True
            elif item_type == "error":
                # Observed in a successful run: a notice, not a failure.
                message = item.get("message")
                if isinstance(message, str):
                    acc.notices.append(message)

        elif event_type == "error" or (
            isinstance(event_type, str) and event_type.endswith(".failed")
        ):
            # Distinct from an `error` *item*, which was observed in a successful run. A top-level
            # error event is the run itself reporting failure, so it is not shrugged off.
            acc.is_error = True
            acc.terminal = event
            message = event.get("message")
            if isinstance(message, str):
                acc.notices.append(message)

    def classify(self, acc: Accumulator, exit_code: int) -> Status:
        # No terminal success/failure event exists, so the exit code is the authority and the closing
        # message is the corroboration.
        if acc.is_error:
            return "failed"
        if exit_code != 0:
            return "failed"
        return "completed" if acc.saw_final_message else "failed"
