"""The PATH wrapper refuses merge-conclusion git forms unless MERGE_HEAD exists."""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from harflow_mcp.merge_gate import apply_path, install


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _commit(repo: Path, name: str) -> None:
    (repo / name).write_text(name + "\n")
    _git(repo, "add", name)
    _git(repo, "commit", "-qm", name)


def _wrapper(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    apply_path(env, install(tmp_path, "task"))
    return env


def _run(repo: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo, env=env, capture_output=True, text=True)


def test_wrapper_refuses_conclusion_without_merge_head_and_passes_other_git(git_repo: Path, tmp_path: Path):
    _commit(git_repo, "a")
    env = _wrapper(tmp_path)
    refused = _run(git_repo, env, "commit", "--no-edit")
    assert refused.returncode != 0
    assert "MERGE_HEAD" in refused.stderr
    status = _run(git_repo, env, "status", "--porcelain")
    assert status.returncode == 0
    also = _run(git_repo, env, "-C", str(git_repo), "commit", "--no-edit")
    assert also.returncode != 0
    assert "MERGE_HEAD" in also.stderr


def test_wrapper_allows_conclusion_only_while_merge_head_exists(git_repo: Path, tmp_path: Path):
    _commit(git_repo, "a")
    _git(git_repo, "checkout", "-q", "-b", "other")
    _commit(git_repo, "b")
    _git(git_repo, "checkout", "-q", "-")
    _git(git_repo, "merge", "--no-commit", "--no-ff", "other")
    assert subprocess.run(["git", "rev-parse", "--verify", "MERGE_HEAD"], cwd=git_repo, capture_output=True).returncode == 0
    env = _wrapper(tmp_path)
    env["GIT_EDITOR"] = "true"
    allowed = _run(git_repo, env, "merge", "--continue")
    assert allowed.returncode == 0, allowed.stderr
    assert subprocess.run(["git", "rev-parse", "--verify", "MERGE_HEAD"], cwd=git_repo, capture_output=True).returncode != 0
