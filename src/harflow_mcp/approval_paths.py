"""Path facts for caller approvals: repository containment and the targets a caller may never approve.

Evaluated at every admission, after symlink resolution. They cannot prevent a later symlink swap.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from . import state_layout

GIT_TIMEOUT_SECONDS = 2
MANAGED_SETTINGS = ("/Library/Application Support/ClaudeCode", "/etc/claude-code")


def resolve(path: str, base: str | Path | None = None) -> Path:
    """Symlink-resolved absolute path; a missing tail resolves against its nearest existing parent."""
    return Path(os.path.realpath(os.path.join(str(base), path) if base is not None else path))


def inside(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _fold(path: Path) -> Path:
    # Case-folded on every volume: over-refusing on a case-sensitive one is the safe error.
    return Path(str(path).casefold())


def case_insensitive(path: Path) -> bool:
    """Whether the volume holding `path` ignores case; unknown means sensitive (compared exactly)."""
    probe = path
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    # Python does not name _PC_CASE_SENSITIVE; it is 11 in Darwin's unistd.h.
    name = "PC_CASE_SENSITIVE" if "PC_CASE_SENSITIVE" in getattr(os, "pathconf_names", {}) else 11 if sys.platform == "darwin" else None
    try:
        return name is not None and os.pathconf(str(probe), name) == 0
    except (AttributeError, ValueError, OSError):
        return False


def inside_repository(repo: str | Path, file_path: str) -> bool:
    """Exact realpath containment, folding case only on a case-insensitive volume. Relative paths are never inside."""
    if not isinstance(file_path, str) or "\x00" in file_path or not os.path.isabs(file_path):
        return False
    target, root = resolve(file_path), resolve(str(repo))
    if case_insensitive(root):
        target, root = _fold(target), _fold(root)
    return inside(target, root)


def _git(repo: Path, *args: str) -> tuple[int, str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_OPTIONAL_LOCKS="0")
    result = subprocess.run(["git", "-C", str(repo), "-c", "core.fsmonitor=false", *args], capture_output=True, text=True, timeout=GIT_TIMEOUT_SECONDS, env=env, check=False)
    return result.returncode, result.stdout, result.stderr


def git_protected(repo: Path) -> list[Path] | None:
    """Effective hooks, common-directory and worktree locations; None when they cannot be resolved."""
    try:
        code, out, err = _git(repo, "rev-parse", "--git-path", "hooks", "--git-common-dir", "--absolute-git-dir")
        if code != 0:
            return [] if "not a git repository" in err.lower() else None
        lines = out.splitlines()
        if len(lines) != 3:
            return None
        hooks, common, gitdir = (Path(line) if os.path.isabs(line) else repo / line for line in lines)
        return [resolve(str(p)) for p in (hooks, common / "hooks", common / "config", gitdir / "hooks", gitdir / "config", gitdir / "config.worktree")]
    except (OSError, subprocess.SubprocessError, ValueError):
        return None


def protected_targets(repo: str | Path, log_dir: Path, environ: Any = None, extra: Any = ()) -> dict[str, Any]:
    """Resolved protected locations for one task; `git` is None when its resolution failed.

    `extra` is the backend's own list (`protected_config_paths`), resolved and folded like the rest;
    it is empty for Claude."""
    env = os.environ if environ is None else environ
    home = Path(env.get("HOME") or Path.home())
    config = Path(env.get("CLAUDE_CONFIG_DIR") or home / ".claude")
    repo_path = Path(str(repo))
    state = [resolve(str(p)) for p in state_layout.protected_roots(log_dir=log_dir)]
    settings = [repo_path / ".claude", repo_path / ".claude" / "settings.json", repo_path / ".claude" / "settings.local.json", repo_path / ".mcp.json",
                config, config / "settings.json", config / "settings.local.json", home / ".claude.json", *map(Path, MANAGED_SETTINGS)]
    return {"state": [_fold(p) for p in state], "settings": [_fold(resolve(str(p))) for p in settings],
            "git": None if (git := git_protected(repo_path)) is None else [_fold(p) for p in git],
            "backend": [_fold(resolve(str(p))) for p in extra]}


def _git_dirs(target: Path) -> list[Path]:
    """Every Git directory above `target` (resolved, unfolded): named `.git`, or holding HEAD with objects/refs or commondir.

    That covers `.git`, bare `*.git`, `.git/modules/<name>` and `.git/worktrees/<name>`."""
    found = []
    for directory in target.parents:
        try:
            head = (directory / "HEAD").is_file()
            if directory.name.casefold() == ".git" or (head and ((directory / "objects").is_dir() and (directory / "refs").is_dir() or (directory / "commondir").is_file())):
                found.append(directory)
        except OSError:
            continue
    return found


def _in_git_dir(target: Path) -> bool:
    for directory in _git_dirs(target):
        parts = tuple(part.casefold() for part in target.relative_to(directory).parts)
        if parts[:1] == ("hooks",) or parts in {("config",), ("config.worktree",)}:
            return True
    return False


def protection(targets: dict[str, Any], tool: str, file_path: str, base: str | Path | None = None, write_tools: Any = frozenset({"Write", "Edit"})) -> str | None:
    """`protected_path` when a caller must not approve this action, else None.

    Tools in `write_tools` (the backend's own set; Write and Edit by default) are checked against every
    protected location; any other tool only against the state root. Unresolvable git locations refuse writes."""
    real = resolve(file_path, base)
    # Also walk the lexical path: a symlinked hooks directory or config file in another repository resolves outside any Git ancestor.
    lexical = Path(os.path.normpath(os.path.join(str(base), file_path) if base is not None else file_path))
    target = _fold(real)
    if any(inside(target, root) for root in targets["state"]):
        return "protected_path"
    if tool not in write_tools:
        return None
    if targets["git"] is None or any(inside(target, root) for root in targets["settings"] + targets["git"] + targets.get("backend", [])) or _in_git_dir(real) or _in_git_dir(lexical):
        return "protected_path"
    return None
