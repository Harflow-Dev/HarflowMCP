"""Exact merge-conclusion grants, and the runtime gate that keeps them from making ordinary commits.

Claude's deny rule beats a matching allow rule, so `git commit --no-edit` can be granted at
`write_in_repo` only by omitting `Bash(git commit:*)` from `--disallowedTools` while that exact
grant is present. `Bash(git push:*)` stays. Other commit forms are not allow-listed, so a headless
run still refuses them as needing approval. That is a narrower claim than
`direct_commit_commands_denied`, which is therefore false when the commit deny is omitted.

The permission layer cannot see `MERGE_HEAD`. Harflow prepends a `git` wrapper to the task's PATH
that refuses `git commit --no-edit` and `git merge --continue` (optionally after one `-C`) unless
`MERGE_HEAD` exists, and execs the real git for every other command. The wrapper is not an OS
boundary: a caller who invokes git by absolute path, or a granted program that does, is outside it.
"""
from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

COMMIT = ("commit", "--no-edit")
MERGE = ("merge", "--continue")
_CAVEAT = (
    "a git commit --no-edit or git merge --continue grant (optionally after one contained -C) "
    "is allow-listed only for a merge already in progress: Harflow's PATH wrapper refuses it unless "
    "MERGE_HEAD exists. When the commit form is granted, Bash(git commit:*) is omitted from the deny "
    "list because a deny beats an allow; Bash(git push:*) stays. git commit -m, git commit -a, a "
    "pathspec, and bash -c remain ungranted, so a headless run still refuses them. direct_commit_commands_denied "
    "is false in that case because the broad commit deny is gone. local_commit stays denied: ordinary "
    "local commits are not authorized. The wrapper is not an OS boundary"
)


class MergeGateError(ValueError):
    """The merge-conclusion gate could not be installed."""


def _rest(tokens: list[str]) -> tuple[str | None, tuple[str, ...]] | None:
    if not tokens or tokens[0] != "git":
        return None
    args = tokens[1:]
    directory = None
    if args[:1] == ["-C"]:
        if len(args) < 3:
            return None
        directory, args = args[1], args[2:]
    return directory, tuple(args)


def conclusion_kind(pattern: str) -> str | None:
    """`commit` or `merge` when `pattern` is one exact merge-conclusion grant."""
    parsed = _rest(pattern.split(" "))
    if parsed is None:
        return None
    _directory, rest = parsed
    if rest == COMMIT:
        return "commit"
    if rest == MERGE:
        return "merge"
    return None


def carves_commit_deny(patterns: tuple[str, ...] | list[str]) -> bool:
    return any(conclusion_kind(pattern) == "commit" for pattern in patterns)


def needs_gate(patterns: tuple[str, ...] | list[str]) -> bool:
    return any(conclusion_kind(pattern) for pattern in patterns)


def report(patterns: tuple[str, ...] | list[str]) -> dict[str, object] | None:
    commands = [pattern for pattern in patterns if conclusion_kind(pattern)]
    if not commands:
        return None
    return {
        "granted": True,
        "commands": commands,
        "requires": "MERGE_HEAD",
        "ordinary_commits": "refused",
        "push_commands_denied": True,
        "commit_deny_omitted": carves_commit_deny(patterns),
        "gate": "path_wrapper",
    }


def caveat() -> str:
    return _CAVEAT


def disallowed_tools(patterns: tuple[str, ...] | list[str], full: str, push_only: str) -> str:
    """The write_in_repo deny list: push only when a `git commit --no-edit` grant is present."""
    return push_only if carves_commit_deny(patterns) else full


def install(log_dir: Path, task_id: str) -> Path:
    """Write a `git` wrapper under the state root and return the directory to prepend to PATH."""
    real = shutil.which("git")
    if not real:
        raise MergeGateError("git is not on PATH, so the merge-conclusion gate cannot be installed")
    directory = log_dir / f"{task_id}.merge-gate"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "git"
    path.write_text(_script(real))
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return directory


def apply_path(env: dict[str, str], directory: Path) -> None:
    current = env.get("PATH", os.environ.get("PATH", ""))
    env["PATH"] = str(directory) + os.pathsep + current


def _script(real: str) -> str:
    return f"""#!{sys.executable}
import os, subprocess, sys
from pathlib import Path
REAL = {real!r}
args = sys.argv[1:]
rest = args
repo = None
if rest[:1] == ["-C"] and len(rest) >= 3:
    repo, rest = rest[1], rest[2:]
if tuple(rest) in (("commit", "--no-edit"), ("merge", "--continue")):
    target = repo or os.getcwd()
    probe = subprocess.run(
        [REAL, "-C", target, "rev-parse", "--path-format=absolute", "--git-path", "MERGE_HEAD"],
        capture_output=True, text=True,
    )
    head = Path(probe.stdout.strip()) if probe.returncode == 0 and probe.stdout.strip() else None
    if head is None or not head.is_file():
        sys.stderr.write(
            "refused: git commit --no-edit and git merge --continue are granted only while MERGE_HEAD exists; "
            "Harflow did not create a commit\\n"
        )
        sys.exit(1)
os.execv(REAL, [REAL, *args])
"""
