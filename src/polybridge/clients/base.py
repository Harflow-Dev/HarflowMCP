"""What every MCP client has in common, and the vocabulary for reporting on one.

A "client" here is something that can be told to launch our stdio server: the Claude desktop app, or
one of the agent CLIs that doubles as an MCP host. They divide into two kinds that share nothing but
this interface — the desktop app owns a JSON file we edit ourselves, while the CLIs own their config
formats and are driven through their own `mcp add` subcommands.

Statuses are deliberately seven, not two. `unknown` exists because a CLI that times out may already
have written the config, and saying `failed` there would be a guess presented as a fact. `removed` and
`not_installed` are the two outcomes of an uninstall that did not go wrong, and they are kept apart
because only one of them changed anything.
"""

from __future__ import annotations

import shlex
import shutil
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

# Generous: `claude mcp add` was measured at a few seconds, but a cold Node start on a loaded machine
# is not something to race.
CLI_TIMEOUT_SECONDS = 60.0

# Enough of a failing command's output to diagnose it, without pasting a screenful into the report.
OUTPUT_TAIL_CHARS = 600


class SetupError(RuntimeError):
    """Something the user needs to fix before setup can proceed."""


Status = Literal[
    "applied", "previewed", "skipped", "failed", "unknown", "removed", "not_installed"
]


@dataclass(frozen=True)
class Registration:
    """What every client is being asked to register: our server, and the PATH it needs to work."""

    key: str
    command: str
    path_env: str


@dataclass(frozen=True)
class RunResult:
    """The outcome of one CLI invocation.

    `returncode is None` means the process never reported one: either it timed out (`timed_out`) or
    it could not be launched at all, which are different situations for the caller.

    `output` is stdout and stderr together, which is what a human diagnosing a failure wants.
    `stdout` alone is kept for callers that parse it — a warning on stderr must not corrupt a JSON
    listing. `None` means it was not captured separately, and `output` is all there is.
    """

    argv: tuple[str, ...]
    returncode: int | None
    output: str
    timed_out: bool = False
    stdout: str | None = None

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    @property
    def tail(self) -> str:
        text = self.output.strip()
        return text if len(text) <= OUTPUT_TAIL_CHARS else "…" + text[-OUTPUT_TAIL_CHARS:]


Runner = Callable[[Sequence[str]], RunResult]


def run_cli(argv: Sequence[str]) -> RunResult:
    """Run one client CLI command.

    `stdin=DEVNULL` is not tidiness: `codex exec` blocks forever reading stdin, and `opencode mcp
    add` is prompt-capable. `cwd` is the user's home so that a project-local config in whatever
    directory the installer happens to be run from cannot capture a registration meant to be global.
    """
    argv = list(argv)
    try:
        completed = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=CLI_TIMEOUT_SECONDS,
            cwd=Path.home(),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return RunResult(tuple(argv), None, "", timed_out=True)
    except OSError as exc:
        return RunResult(tuple(argv), None, f"could not run {argv[0]}: {exc}")
    return RunResult(
        tuple(argv),
        completed.returncode,
        completed.stdout + completed.stderr,
        stdout=completed.stdout,
    )


@dataclass(frozen=True)
class Availability:
    """Whether this client can be registered with at all, and where it was found.

    `errored` separates "this client is not installed" from "asking whether it is installed broke".
    Both leave us unable to proceed, but only the first is an ordinary skip; the second is a fault,
    and letting it exit zero as "not installed" would hide it.
    """

    available: bool
    where: str | None = None
    reason: str | None = None
    errored: bool = False


@dataclass(frozen=True)
class Result:
    """What happened for one client. `detail` is one line; `diagnostics` are extra lines beneath it."""

    client: str
    status: Status
    detail: str
    steps: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = ()


@dataclass(frozen=True)
class Inspection:
    """What one client's config says about our server, read without changing anything.

    `installed is None` means "could not tell" — the check was not run, or it errored (`error` says
    which). It is never folded into False: "not registered" and "could not look" are different answers,
    and the Mac app acts on the first.

    `command` and `path_env` are what the stored entry would launch, so a caller can compare them with
    what install would write now. `available` is filled in by the orchestration, not by the client.
    """

    client: str
    installed: bool | None
    command: str | None = None
    path_env: str | None = None
    error: str | None = None
    notes: tuple[str, ...] = ()
    available: bool = True


def entry_inspection(client: str, entry: object, where: str) -> Inspection:
    """Read a `{"command": …, "args": […], "env": {"PATH": …}}` entry — the shape the desktop app,
    Claude Code and vibe all store, and the one Codex's `mcp list --json` reports under `transport`.

    A non-empty `args` is folded into `command` rather than dropped: the entry launches more than the
    bare command, so reporting the command alone would let it compare equal to one that does not.
    """
    if not isinstance(entry, dict):
        return Inspection(
            client, True, error=f"the entry in {where} is not an object", notes=(f"read {where}",)
        )
    command = entry.get("command")
    args = entry.get("args")
    env = entry.get("env")
    path_env = env.get("PATH") if isinstance(env, dict) else None
    if isinstance(command, str) and args:
        command = shlex.join([command, *(str(arg) for arg in args)]) if isinstance(args, list) else None
    return Inspection(
        client,
        True,
        command=command if isinstance(command, str) else None,
        path_env=path_env if isinstance(path_env, str) else None,
        notes=(f"read {where}",),
    )


class Client(Protocol):
    """Everything the orchestration needs. Nothing outside `clients/` branches on `key`.

    `post_apply_note` is here so that "restart the desktop app, but not the CLIs" is knowledge each
    client holds about itself, rather than a name check in the reporting code. It is phrased to
    follow the client's own label, and it applies to a removal as much as to an add.

    `inspect` and `remove` take the server's key rather than a `Registration`: neither needs the
    server binary, and an uninstall has to work after that binary is gone.
    """

    key: str
    label: str
    post_apply_note: str

    def availability(self) -> Availability: ...

    def preview(self, registration: Registration) -> Result: ...

    def apply(self, registration: Registration, run: Runner) -> Result: ...

    def inspect(self, key: str, run: Runner) -> Inspection: ...

    def remove(self, key: str, run: Runner) -> Result: ...


@dataclass(frozen=True)
class CliClient:
    """A client registered by invoking its own `mcp add`, because it owns its config format.

    Editing those files ourselves would mean round-tripping a 78 KB store of application state, a
    hand-commented TOML file, and JSONC — each client's own CLI already does that correctly.

    The default policy assumes `add` overwrites an existing entry, which is what Codex and opencode
    were measured to do. A client whose `add` refuses instead must say so by overriding `apply`.
    """

    key: str
    label: str
    binary: str
    config_hint: str
    add_flags: tuple[str, ...] = ()
    # These read their config when a session starts, so there is nothing to restart.
    post_apply_note: str = "no restart needed; a new session reads its config."

    def add_argv(self, registration: Registration) -> list[str]:
        return [
            self.binary,
            "mcp",
            "add",
            registration.key,
            *self.add_flags,
            *self.env_flag(registration),
            "--",
            registration.command,
        ]

    def env_flag(self, registration: Registration) -> list[str]:
        """How this CLI spells "set an environment variable for the server it launches"."""
        raise NotImplementedError

    def inspect(self, key: str, run: Runner) -> Inspection:
        """How to read this CLI's registration differs per CLI; there is no safe shared default."""
        raise NotImplementedError(f"{self.label} does not implement inspect")

    def remove(self, key: str, run: Runner) -> Result:
        """Neither is there one for removal: each CLI reports absence differently, or not at all."""
        raise NotImplementedError(f"{self.label} does not implement remove")

    def availability(self) -> Availability:
        found = shutil.which(self.binary)
        if found is None:
            return Availability(False, reason=f"`{self.binary}` is not on PATH")
        return Availability(True, where=found)

    def preview(self, registration: Registration) -> Result:
        argv = shlex.join(self.add_argv(registration))
        return Result(self.key, "previewed", f"would run: {argv}", steps=(argv,))

    def apply(self, registration: Registration, run: Runner) -> Result:
        return self.judge(run(self.add_argv(registration)))

    def judge(self, added: RunResult) -> Result:
        """Turn one `add` invocation into a reportable outcome.

        The success wording is deliberately weak. All that was observed is that the command exited
        zero — not that the client loaded the server, nor that it can launch it.
        """
        steps = (shlex.join(added.argv),)
        if added.ok:
            return Result(
                self.key, "applied", f"add command succeeded ({self.config_hint})", steps=steps
            )
        if added.timed_out:
            return Result(
                self.key,
                "unknown",
                f"timed out after {CLI_TIMEOUT_SECONDS:.0f}s; the config may have changed",
                steps=steps,
            )
        return Result(
            self.key,
            "failed",
            f"add command failed (exit {added.returncode})",
            steps=steps,
            diagnostics=(added.tail,) if added.tail else (),
        )
