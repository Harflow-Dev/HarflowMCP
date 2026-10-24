"""Claude `allowed_commands` grammar.

A best-effort guard against caller mistakes and obvious escalation, not a security boundary: the
authority boundaries are the caller who writes the grant, the nested-subset rule and the access
level's own deny patterns, which still win (measured). Anything not matching is refused.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from ..command_grants import CommandGrantError

FREEDOMS = ("write_in_repo", "publish")
MAX_LAUNCHER_DEPTH = 2

_CHARS = re.compile(r"[A-Za-z0-9 _./:=@%+*-]+")
_PROGRAM = re.compile(r"[a-z0-9][a-z0-9._+-]*")
_OPTION = re.compile(r"--[A-Za-z0-9][A-Za-z0-9_-]*")
_MODULE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*")
_SCRIPT = re.compile(r"[A-Za-z0-9_./+-]*\.(py|js|mjs|cjs|ts|mts|cts|jsx|tsx)")
_PYTHON = re.compile(r"(python|pypy)\d*(\.\d+)*")
_INTERPRETER = re.compile(r"(python|pypy|node)\d*(\.\d+)*|nodejs|deno|bun|tsx|ts-node")
_REFUSED_MODULES = frozenset({"code", "codeop", "pdb", "runpy", "cProfile", "profile", "trace", "timeit", "idlelib"})

_REFUSED_GROUPS = {
    "a shell or command evaluator": "bash sh zsh dash ksh fish csh tcsh env exec eval command builtin source pwsh nu xonsh busybox toybox",
    "privilege escalation or remote execution": "sudo doas su ssh ssh-agent scp rsync",
    "a wrapper that runs other commands": "nohup nice timeout time watch xargs parallel arch caffeinate chroot setsid stdbuf unbuffer script sandbox-exec flock ionice taskset strace dtruss gdb lldb tmux screen valgrind cross-env direnv",
    "a program that evaluates code": "osascript open awk gawk mawk nawk sed perl ruby php lua tclsh expect java jshell swift irb Rscript vim vi nvim ex ed emacs nano",
    "a program that changes system state or other processes": "crontab launchctl defaults security kill pkill killall",
    "container, remote and flake launchers are not supported": "docker podman kubectl nix",
}
REFUSED_PROGRAMS = {name: reason for reason, names in _REFUSED_GROUPS.items() for name in names.split()}
_GIT_REASON = "git is refused at every access level: publish already allows git commit and git push, read-only git needs no grant, and aliases make a subcommand list incomplete"

# Exact-only: no wildcard, and every operand (bare names included) must resolve inside the repository.
_PATH_SENSITIVE = frozenset("cp mv tee install touch mkdir truncate dd diskutil chmod chown chflags rm rmdir shred unlink ln".split())
_FIND_REFUSED = frozenset({"-exec", "-execdir", "-ok", "-okdir", "-delete", "-fprint", "-fprint0", "-fprintf", "-fls"})

_LAUNCHERS = (("npx",), ("uvx",), ("bunx",), ("pnpx",))
# Programs that own subcommands. After the allowed directory options the next token must be a literal subcommand from
# a positive list: launcher subcommands (the launched command is validated recursively), known non-launching
# built-ins, and `run`-style subcommands that need a literal script name. Everything else is refused.
# program -> (launcher subcommands, built-in subcommand sequences, script-running subcommands, bare token is a binary to launch)
_OWNERS = {
    "uv": ({"run"}, {("sync",), ("lock",), ("build",), ("pip", "install")}, set(), False),
    "npm": ({"exec", "x"}, {("test",), ("install",), ("ci",)}, {"run"}, False),
    "pnpm": ({"exec", "dlx"}, {("install",), ("i",), ("test",), ("t",)}, {"run"}, False),
    "yarn": ({"exec", "dlx"}, {("install",), ("test",)}, {"run"}, True),
    "pipx": ({"run"}, set(), set(), False),
    "poetry": ({"run"}, {("install",), ("build",)}, set(), False),
    "bundle": ({"exec"}, {("install",)}, set(), False),
    "mise": ({"exec", "x"}, {("install",)}, set(), False),
    "asdf": ({"exec"}, set(), set(), False),
    "pdm": ({"run"}, {("install",)}, set(), False),
    "hatch": ({"run"}, {("build",)}, set(), False),
    "pipenv": ({"run"}, {("install",)}, set(), False),
    "conda": ({"run"}, set(), set(), False),
    "pixi": ({"run"}, {("install",)}, set(), False),
    "rye": ({"run"}, {("sync",), ("build",)}, set(), False),
}
# Yarn runs a bare token as a package binary; these are its own subcommands, which are not accepted.
_YARN_BUILTINS = frozenset({"create", "global", "workspace", "workspaces", "plugin", "set", "config", "init", "node"})
_DIR_OPTIONS = frozenset({"--cwd", "--directory", "--dir", "--prefix", "-C"})
_GH_PAIRS = frozenset({("workflow", "run"), ("workflow", "view"), ("run", "view"), ("run", "watch"), ("run", "list"),
                       ("pr", "create"), ("pr", "view"), ("pr", "list"), ("pr", "checks"), ("pr", "status")})

GRAMMAR = {
    "freedoms": list(FREEDOMS),
    "characters": "[A-Za-z0-9 _./:=@%+-] and a single *",
    "wildcard": "`*` only as a separate final token (`cmd sub *`) or as the `:*` suffix (`cmd sub:*`); a wildcard needs a literal non-option token after the program",
    "program": "a lowercase bare name; shells, wrappers, evaluators, system tools, git, docker, podman, kubectl and nix are refused",
    "paths": "tokens containing / must be plain relative paths or absolute paths that resolve inside the repository, optionally as --option=path",
    "interpreters": "python -m <module> or <interpreter> <script with a script extension>",
    "launchers": f"the launched command is validated recursively, at most {MAX_LAUNCHER_DEPTH} deep",
    "gh": "publish only, for a fixed list of subcommand pairs",
}


class _Refusal(Exception):
    pass


def validate(pattern: str, freedom: str, repo: Path | str | None) -> None:
    """Raise `CommandGrantError` naming `pattern` and the reason; `repo=None` skips only path resolution."""
    try:
        _validate(pattern, freedom, None if repo is None else os.path.realpath(repo))
    except _Refusal as exc:
        raise CommandGrantError(f"allowed_commands pattern {pattern!r} refused: {exc}") from None


def validate_all(patterns, freedom: str, repo: Path | str | None) -> None:
    for pattern in patterns:
        validate(pattern, freedom, repo)


def _validate(pattern: str, freedom: str, root: str | None) -> None:
    if freedom not in FREEDOMS:
        raise _Refusal(f"allowed_commands is supported only at write_in_repo and publish, not {freedom}")
    if not _CHARS.fullmatch(pattern):
        bad = next(ch for ch in pattern if not _CHARS.fullmatch(ch))
        why = "a comma would split the comma-joined --allowedTools value" if bad == "," else f"{bad!r} is not an allowed character (allowed: letters, digits, space and _ . / : = @ % + - *)"
        raise _Refusal(why)
    if pattern != pattern.strip() or "  " in pattern:
        raise _Refusal("tokens must be separated by single spaces")
    _tokens(pattern.split(" "), freedom, root, 0)


def _wildcard(tokens: list[str]) -> tuple[list[str], bool]:
    if "*" not in "".join(tokens):
        return tokens, False
    if "".join(tokens).count("*") != 1:
        raise _Refusal("at most one * is accepted")
    last = tokens[-1]
    if last == "*":
        if len(tokens) < 2:
            raise _Refusal("a wildcard cannot replace the program")
        return tokens[:-1], True
    if last.endswith(":*") and len(last) > 2:
        return [*tokens[:-1], last[:-2]], True
    raise _Refusal("* is accepted only as a separate final token (`cmd sub *`) or as the `:*` suffix (`cmd sub:*`), never inside a token; write `pytest:*` rather than `pytest*`")


def _launcher(literal: list[str], root: str | None) -> int:
    """Tokens consumed before a launched command (0 when the pattern launches nothing); refuses anything outside the allowlist."""
    program = literal[0]
    if (program,) in _LAUNCHERS:
        return 1
    if program not in _OWNERS:
        return 0
    launchers, builtins, scripts, bins = _OWNERS[program]
    index = 1
    while index < len(literal):
        token = literal[index]
        if token in _DIR_OPTIONS:
            if index + 1 >= len(literal):
                raise _Refusal(f"option {token!r} needs a directory value")
            _check_path(literal[index + 1], root)
            index += 2
        elif token.startswith("--") and token.partition("=")[0] in _DIR_OPTIONS and "=" in token:
            _check_path(token.partition("=")[2], root)
            index += 1
        elif token.startswith("-"):
            raise _Refusal(f"option {token!r} before the subcommand of {program!r} is refused; only {', '.join(sorted(_DIR_OPTIONS))} with a path are accepted there")
        else:
            break
    if index >= len(literal):
        raise _Refusal(f"{program!r} needs a literal subcommand after its directory options")
    sub = literal[index]
    if sub in launchers:
        return index + 1
    if program == "yarn" and sub == "node":
        _interpreter("node", literal[index + 1:])
        return 0
    if any(tuple(literal[index:index + len(builtin)]) == builtin for builtin in builtins):
        return 0
    if sub in scripts:
        if not any(not token.startswith("-") for token in literal[index + 1:]):
            raise _Refusal(f"{program} {sub} needs a literal script name")
        return 0
    if bins and sub not in _YARN_BUILTINS:
        return index
    allowed = sorted({*launchers, *scripts, *(b[0] for b in builtins)})
    raise _Refusal(f"{program!r} subcommand {sub!r} is not accepted; accepted: {', '.join(allowed)}")


def _tokens(tokens: list[str], freedom: str, root: str | None, depth: int, lenient: bool = False) -> None:
    literal, wildcard = _wildcard(tokens)
    program = literal[0]
    if not _PROGRAM.fullmatch(program):
        raise _Refusal(f"the program {program!r} must be a lowercase bare name without a path, quotes or environment assignment")
    prefix = _launcher(literal, root)
    if prefix:
        if depth >= MAX_LAUNCHER_DEPTH:
            raise _Refusal(f"launchers may nest at most {MAX_LAUNCHER_DEPTH} deep")
        rest = tokens[prefix:]
        if not rest:
            raise _Refusal("a launcher needs a launched command")
        if rest[0].startswith("-"):
            if "--" not in rest:
                raise _Refusal("launcher options must be followed by `--` and the launched command")
            split = rest.index("--")
            for token in rest[:split]:
                _path(token, root)
            rest = rest[split + 1:]
            if not rest:
                raise _Refusal("a launcher needs a launched command after `--`")
        _tokens(rest, freedom, root, depth + 1, lenient=program == "yarn" and literal[prefix - 1] not in _OWNERS["yarn"][0])
        return
    if "--" in tokens:
        raise _Refusal("`--` is accepted only as a launcher separator")
    if program.startswith("git-"):
        raise _Refusal(_GIT_REASON)
    if program in REFUSED_PROGRAMS:
        raise _Refusal(f"{program!r} is refused: {REFUSED_PROGRAMS[program]}")
    if program == "git":
        raise _Refusal(_GIT_REASON)
    if program == "gh":
        if freedom != "publish":
            raise _Refusal("gh is refused below publish")
        if tuple(literal[1:3]) not in _GH_PAIRS:
            raise _Refusal("gh is accepted only as one of: " + ", ".join(f"gh {a} {b}" for a, b in sorted(_GH_PAIRS)))
    if program == "cmake" and any(token in ("-E", "-P") or token.startswith(("-E", "-P")) for token in literal[1:]):
        raise _Refusal("cmake -E and -P run commands and scripts")
    if wildcard and not lenient and not any(not token.startswith("-") for token in literal[1:]):
        raise _Refusal("a wildcard pattern needs a literal non-option token after the program (a subcommand or target, such as `make test:*`); option values are not parsed")
    exact_only = program in _PATH_SENSITIVE or program.startswith("mkfs")
    if exact_only and wildcard:
        raise _Refusal(f"{program!r} writes to the filesystem and accepts only exact patterns")
    if program == "find" and (wildcard or _FIND_REFUSED.intersection(literal)):
        raise _Refusal("find is refused with a wildcard or with -exec, -execdir, -ok, -okdir, -delete or file-writing actions")
    if program == "make":
        if all(token.startswith("-") for token in literal[1:]):
            raise _Refusal("make needs a literal target")
        if any(token == "-E" or token.startswith(("-E", "--eval")) for token in literal[1:]):
            raise _Refusal("make --eval and -E evaluate makefile text")
    if _INTERPRETER.fullmatch(program):
        _interpreter(program, literal[1:])
    for token in literal[1:]:
        _path(token, root)
        if exact_only and not token.startswith("-"):
            _check_path(token, root)
            if program == "dd" and "=" in token:
                _check_path(token.partition("=")[2], root)
    if exact_only:
        for token in literal[1:]:
            if token.startswith("-") and not token.startswith("--") and len(token) > 2:
                _check_path(token[2:], root)
            elif token.startswith("--") and "=" in token:
                _check_path(token.partition("=")[2], root)


def _interpreter(program: str, rest: list[str]) -> None:
    if rest and _PYTHON.fullmatch(program) and rest[0] == "-m":
        if len(rest) < 2 or not _MODULE.fullmatch(rest[1]):
            raise _Refusal("`-m` needs a literal module name")
        if rest[1].split(".")[0] in _REFUSED_MODULES:
            raise _Refusal(f"module {rest[1]!r} evaluates code or runs arbitrary scripts")
        return
    if rest and not rest[0].startswith("-") and _SCRIPT.fullmatch(rest[0]):
        return
    raise _Refusal(f"interpreter {program!r} is accepted only as `-m <module>` (Python) or `<script path with a script extension>`; no other option or subcommand may precede it")


def _path(token: str, root: str | None) -> None:
    value = token
    if token.startswith("--") and "=" in token:
        head, _, value = token.partition("=")
        if not _OPTION.fullmatch(head):
            raise _Refusal(f"token {token!r} is not a plain --option=value")
    elif "/" in token and (token.startswith("-") or "=" in token):
        raise _Refusal(f"token {token!r} contains / but is not a plain path or --option=path")
    if "/" in value and "=" in value:
        raise _Refusal(f"token {token!r} contains / but is not a plain path or --option=path")
    if "/" in value or value in (".", ".."):
        _check_path(value, root)


def _check_path(value: str, root: str | None) -> None:
    segments = value.split("/")
    if value.startswith("/"):
        segments = segments[1:]
    if segments and segments[-1] == "":
        segments = segments[:-1]
    if any(segment in ("", "..") for segment in segments):
        raise _Refusal(f"path {value!r} has empty or `..` segments")
    if root is None:
        return
    target = os.path.realpath(value if value.startswith("/") else os.path.join(root, value))
    if target != root and not target.startswith(root.rstrip(os.sep) + os.sep):
        raise _Refusal(f"path {value!r} resolves outside the repository")
