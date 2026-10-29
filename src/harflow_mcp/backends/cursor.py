"""Cursor Agent CLI, measured on 2026.10.22-27dcf5f, macOS arm64, 2026-10-28.

Headless launch is `cursor-agent -p` with the prompt after `--`. A prompt that
looks like a flag stays one positional. Prompts over 32 KiB go on stdin, which
this CLI trims, so outer whitespace is refused there. There is no live follow-up
protocol: stdin is one shot and then closed.

`--mode plan` and `--mode ask` are accepted by the parser. On one authenticated
run, plan mode declined the assignment without tool calls, and ask mode read a
file and did not write. Default print mode wrote inside the repo and outside it.
`--sandbox enabled` also wrote outside the repo and created a commit. Those runs
do not establish an OS boundary, so no freedom claims `writes_confined` or
`os_enforced`. `write_in_repo`, `publish`, and `unrestricted` share one argv.

`--effort` is not a flag. `--system-prompt` is a file path whose delivery into a
request was not observed, so system guidance stays unset. `--new-session-id`
exists and wants a UUIDv4, but resume of an unknown id was not observed past
auth, and the shared tests address sessions the CLI itself disclosed, so this
backend does not choose the id up front. `resume_may_start_fresh` stays true.

Signing into the Cursor editor does not sign in this CLI. `agent status` has to
report an access token for the same HOME, via `agent login` or CURSOR_API_KEY.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .base import (
    FREEDOMS,
    Accumulator,
    Capabilities,
    Enforcement,
    ExtraPathsUnsupported,
    Freedom,
    Invocation,
    NetworkControl,
    ReasoningEffort,
    check_freedom,
    check_network,
    check_reasoning_effort,
    classic_invocation_problem,
    interactive_session_id_ok,
    reject_model,
    reject_turn_cap,
    UnsupportedCapability,
)
from .config_namespaces import project_namespaces
from .mcp_approval import CursorApproval

BINARY = "cursor-agent"
ARGV_PROMPT_BYTES = 32 * 1024
_COLLAPSE = "cannot tell these two freedoms apart"
_NO_BOUNDARY = (
    "Cursor print mode has no measured OS sandbox. A 2026-10-28 run of "
    f"{BINARY} -p wrote a file inside the repo and a file outside it. "
    "--sandbox enabled also wrote outside the repo and created a commit. "
    "Commit, push, and network were not shown to be blocked."
)
_COLLAPSE_CAVEAT = (
    "write_in_repo, publish, and unrestricted share one print-mode argv, so "
    f"Harflow {_COLLAPSE}."
)
_PLAN_CAVEAT = (
    "read_only adds --mode plan. On one 2026-10-28 run the model declined the "
    "assignment without tool calls. Ask mode, which Harflow does not launch, "
    "read a file and did not write. That is model restraint on those runs, not "
    "an OS sandbox."
)
_NETWORK_CAVEAT = (
    "No sandbox flag Harflow sends was measured as a network barrier. "
    "A request to block the network is refused. Allowing it changes no argv, "
    "because there is no barrier to remove."
)
_EFFORT_CAVEAT = (
    "Cursor Agent CLI 2026.10.22-27dcf5f has no --effort flag. "
    "Harflow does not encode effort into the model id."
)

BOOLEAN_FLAGS = frozenset({"-p", "--trust"})
VALUE_FLAGS = frozenset({"--output-format", "--workspace", "--mode", "--model", "--resume"})
REJECTED_FLAGS = frozenset({
    "--force", "--yolo", "--sandbox", "--effort", "--worktree", "--add-dir",
    "--continue", "--system-prompt", "--new-session-id", "--approve-mcps", "--print",
})


class UnsafeInvocationError(RuntimeError):
    """The argv is not one this backend would have written for that freedom."""


class CursorBackend(ExtraPathsUnsupported):
    name = "cursor"
    binary = BINARY
    mcp_approval = CursorApproval()
    capabilities = Capabilities(
        supports_auto_continue=False,
        chooses_session_id=False,
        supports_turn_cap=False,
        reports_cost_usd=False,
        os_sandbox=False,
        per_command_deny=False,
        supports_model_selection=True,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=False,
            levels=(),
            native_flag="",
            accepted_in_real_run=False,
            levels_change_behaviour=False,
            caveats=(_EFFORT_CAVEAT,),
        ),
        network_control=NetworkControl(
            can_enable=FREEDOMS,
            can_block=(),
            caveats=(_NETWORK_CAVEAT,),
        ),
        supports_live_input=False,
        resume_may_start_fresh=True,
    )

    def execution_configuration_namespaces(self, repo: Path) -> dict[str, tuple[Path, ...]]:
        return {
            "external": (Path.home() / ".cursor",),
            "project": project_namespaces(repo, (".cursor",)),
        }

    def verify_launch_identity(self) -> None:
        from ..cursor_cli import require_version

        require_version(self.binary)

    def build_start_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str | None,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> Invocation:
        if session_id is not None:
            raise ValueError("cursor discloses its own session id; one cannot be supplied")
        invocation = self._invocation(
            self._options(repo, freedom, model, max_turns, reasoning_effort, network),
            prompt,
        )
        self.assert_safe(invocation, freedom, network)
        return invocation

    def build_resume_argv(
        self,
        prompt: str,
        *,
        repo: Path,
        freedom: Freedom,
        session_id: str,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None = None,
    ) -> Invocation:
        if not interactive_session_id_ok(session_id, repo):
            raise ValueError("resuming cursor needs the session id its first run reported")
        options = [
            *self._options(repo, freedom, model, max_turns, reasoning_effort, network),
            "--resume",
            session_id,
        ]
        invocation = self._invocation(options, prompt)
        self.assert_safe(invocation, freedom, network)
        return invocation

    def _options(
        self,
        repo: Path,
        freedom: Freedom,
        model: str | None,
        max_turns: int | None,
        reasoning_effort: str | None,
        network: bool | None,
    ) -> list[str]:
        check_freedom(freedom)
        reject_model(self, model)
        reject_turn_cap(self, max_turns)
        check_reasoning_effort(self, reasoning_effort)
        check_network(self, freedom, network)
        if not Path(repo).is_absolute():
            raise ValueError("repo must be an absolute path")
        options = [
            "-p",
            "--output-format",
            "stream-json",
            "--trust",
            "--workspace",
            str(repo),
        ]
        if freedom == "read_only":
            options += ["--mode", "plan"]
        if model is not None:
            options += ["--model", model]
        return options

    def _invocation(self, options: list[str], prompt: str) -> Invocation:
        if not prompt or not prompt.strip():
            raise ValueError("prompt must be a non-empty string")
        argv = [BINARY, *options, "--"]
        if len(prompt.encode("utf-8")) > ARGV_PROMPT_BYTES:
            if prompt != prompt.strip():
                raise UnsupportedCapability(
                    "Cursor trims outer whitespace from stdin prompts. "
                    "Remove outer whitespace or choose another harness for this large assignment."
                )
            return Invocation(
                argv,
                stdin_mode="pipe_once",
                initial_input=prompt.encode("utf-8"),
            )
        return Invocation([*argv, prompt])

    def assert_safe(self, invocation: Invocation, freedom: Freedom, network: bool | None = None) -> None:
        if isinstance(invocation, Invocation) and invocation.allowed_commands:
            raise UnsupportedCapability(self.allowed_commands_support()["reason"])
        if isinstance(invocation, Invocation) and invocation.extra_writable_paths:
            raise UnsupportedCapability("bounded extra writable paths are not certified for this backend")
        check_freedom(freedom)
        check_network(self, freedom, network)
        one_shot = isinstance(invocation, Invocation) and invocation.stdin_mode == "pipe_once"
        if one_shot:
            try:
                prompt = invocation.initial_input.decode("utf-8")
            except (AttributeError, UnicodeError):
                prompt = ""
            if not prompt or prompt != prompt.strip():
                raise UnsafeInvocationError("Cursor one-shot stdin requires exact nonempty UTF-8 with no outer whitespace")
        else:
            problem = classic_invocation_problem(invocation)
            if problem is not None:
                raise UnsafeInvocationError(problem)
        from .base import validate_invocation_env

        validate_invocation_env(invocation, {})
        argv = invocation.argv
        if "--" not in argv:
            raise UnsafeInvocationError(f"cursor prompt must follow --: {argv!r}")
        split = argv.index("--")
        if argv.index("--") != split or argv.count("--") != 1:
            raise UnsafeInvocationError(f"expected exactly one --: {argv!r}")
        options, prompt_args = argv[1:split], argv[split + 1 :]
        if one_shot:
            if prompt_args:
                raise UnsafeInvocationError("a one-shot stdin prompt must not also be an argv token")
        elif len(prompt_args) != 1 or not prompt_args[0].strip():
            raise UnsafeInvocationError(f"expected one non-empty prompt after --: {argv!r}")
        if not one_shot and len(prompt_args[0].encode("utf-8")) > ARGV_PROMPT_BYTES:
            raise UnsafeInvocationError("Cursor large assignments require one-shot stdin transport")
        if not argv or argv[0] != BINARY:
            raise UnsafeInvocationError(f"unrecognised cursor argv layout: {argv!r}")
        seen = self._parse_options(options, argv)
        if seen.get("-p") != [""] or seen.get("--trust") != [""]:
            raise UnsafeInvocationError(f"expected one -p and one --trust: {argv!r}")
        if seen.get("--output-format") != ["stream-json"]:
            raise UnsafeInvocationError(f"--output-format must be stream-json: {argv!r}")
        workspace = seen.get("--workspace", [])
        if len(workspace) != 1 or not Path(workspace[0]).is_absolute():
            raise UnsafeInvocationError(f"--workspace must be one absolute path: {argv!r}")
        mode = seen.get("--mode", [])
        if freedom == "read_only":
            if mode != ["plan"]:
                raise UnsafeInvocationError(f"read_only requires --mode plan: {argv!r}")
        elif mode:
            raise UnsafeInvocationError(f"{freedom} must not set --mode: {argv!r}")
        resumes = seen.get("--resume", [])
        if len(resumes) > 1 or (resumes and not interactive_session_id_ok(resumes[0], Path(workspace[0]))):
            raise UnsafeInvocationError(f"--resume is missing or repeated: {argv!r}")
        models = seen.get("--model", [])
        if len(models) > 1 or (models and (not models[0] or models[0].startswith("-"))):
            raise UnsafeInvocationError(f"--model must be one value: {argv!r}")

    @staticmethod
    def _parse_options(options: list[str], argv: list[str]) -> dict[str, list[str]]:
        seen: dict[str, list[str]] = {}
        index = 0
        while index < len(options):
            token = options[index]
            if token in REJECTED_FLAGS:
                raise UnsafeInvocationError(f"{token} is not part of this backend's launch: {argv!r}")
            if token in BOOLEAN_FLAGS:
                seen.setdefault(token, []).append("")
                index += 1
                continue
            if token in VALUE_FLAGS:
                if index + 1 >= len(options) or options[index + 1].startswith("-"):
                    raise UnsafeInvocationError(f"{token} has no value: {argv!r}")
                seen.setdefault(token, []).append(options[index + 1])
                index += 2
                continue
            raise UnsafeInvocationError(f"unrecognised option token {token!r}: {argv!r}")
        return seen

    def enforcement(self, freedom: Freedom, network: bool | None = None) -> Enforcement:
        check_network(self, freedom, network)
        caveats = [_NO_BOUNDARY]
        if freedom == "read_only":
            caveats.append(_PLAN_CAVEAT)
            mechanism = f"{BINARY} -p --mode plan"
        else:
            caveats.append(_COLLAPSE_CAVEAT)
            mechanism = f"{BINARY} -p"
        return Enforcement(
            freedom=freedom,
            mechanism=mechanism,
            os_enforced=False,
            writes_confined=False,
            writable_roots=(),
            commit_push_blocked=False,
            direct_commit_commands_denied=False,
            # write_in_repo, publish and unrestricted share one argv, so no commit policy is claimed.
            local_commit="not_enforced",
            publish_attempts_allowed_by_polybridge=freedom in ("publish", "unrestricted"),
            network_access="not_controlled",
            caveats=tuple(caveats),
        )

    def ingest(self, event: dict[str, Any], acc: Accumulator) -> None:
        session_id = event.get("session_id")
        if acc.session_id is None and isinstance(session_id, str) and session_id:
            acc.session_id = session_id
        kind = event.get("type")
        if kind == "result":
            acc.terminal = event
            if event.get("is_error") is True or event.get("subtype") not in (None, "success"):
                acc.is_error = True
                return
            if event.get("subtype") == "success" and event.get("is_error") is False:
                acc.saw_final_message = True
                result = event.get("result")
                if isinstance(result, str) and result:
                    acc.summary = result
            return
        if kind != "assistant":
            return
        message = event.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            return
        texts = [
            part.get("text")
            for part in content
            if isinstance(part, dict) and part.get("type") == "text" and isinstance(part.get("text"), str)
        ]
        if texts and acc.summary is None:
            acc.summary = "\n".join(texts)

    def classify(self, acc: Accumulator, exit_code: int | None) -> str:
        if exit_code is None or exit_code != 0 or acc.is_error:
            return "failed"
        if acc.saw_final_message:
            return "completed"
        return "failed"

    def normalize(self, event: dict[str, Any], acc: Accumulator) -> list[dict[str, Any]]:
        from . import normalize as nz

        if event.get("type") != "assistant":
            return []
        message = event.get("message")
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            return []
        return [
            nz.assistant_text(part["text"])
            for part in content
            if isinstance(part, dict)
            and part.get("type") == "text"
            and isinstance(part.get("text"), str)
        ]

    def continuation_reason(self, acc: Accumulator, exit_code: int | None) -> str | None:
        return None

    def encode_live_message(self, text: str) -> bytes:
        raise UnsupportedCapability(
            f"the {self.name} backend has no live input, so a message cannot be added to a running "
            "task; continue its session with resume_task instead"
        )

    def interactive_resume_argv(self, session_id: str, repo_path: Path) -> list[str] | None:
        if not interactive_session_id_ok(session_id, repo_path):
            return None
        return [self.binary, "--trust", "--workspace", str(repo_path), "--resume", session_id]

    @staticmethod
    def workflow_stderr_availability_failure(diagnostic: str) -> str | None:
        from .workflow_diagnostics import stderr_availability

        return stderr_availability(diagnostic)

    @staticmethod
    def usage_limit_diagnostic(event: dict[str, Any]) -> dict[str, Any] | None:
        from .workflow_diagnostics import usage_limit

        return usage_limit(event, envelope="error", claude=False, agy=False)

    @staticmethod
    def stderr_usage_limit_diagnostic(line: str) -> dict[str, Any] | None:
        # No Cursor stderr quota signal has been qualified.
        return None

    @staticmethod
    def workflow_stderr_blocks_availability_failure(diagnostic: str) -> bool:
        return False


