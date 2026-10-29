"""Harflow's own fetch and branch preparation, before any agent starts."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from harflow_mcp.backends.codex import CodexBackend
from harflow_mcp.dispatch_git import DispatchGitError, prepare, preview


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)


def _commit(repo: Path, name: str = "a") -> str:
    (repo / name).write_text(name + "\n")
    _git(repo, "add", name)
    _git(repo, "commit", "-qm", name)
    return _git(repo, "rev-parse", "HEAD").stdout.strip()


def _remote(tmp_path: Path, repo: Path, branch: str = "develop") -> None:
    bare = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", str(bare)], check=True)
    _git(repo, "remote", "add", "origin", str(bare))
    _git(repo, "push", "-q", "origin", f"HEAD:refs/heads/{branch}")


def _head(repo: Path) -> str:
    return _git(repo, "rev-parse", "HEAD").stdout.strip()


def _branch(repo: Path) -> str:
    return _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()


def test_branch_preparation_creates_from_fetched_base(git_repo: Path, tmp_path: Path):
    _commit(git_repo)
    _remote(tmp_path, git_repo)
    report = prepare(git_repo, freedom="write_in_repo", branch="feature/x", base="origin/develop")
    assert report is not None
    assert report["created"] is True and report["reused"] is False
    assert report["branch"] == "feature/x" and report["base"] == "origin/develop"
    assert report["base_commit"] == _head(git_repo)
    assert _branch(git_repo) == "feature/x"
    assert report["fetch"][0]["remote"] == "origin" and report["fetch"][0]["ok"] is True


def test_dirty_checkout_existing_branch_and_bad_names_refuse(git_repo: Path, tmp_path: Path):
    _commit(git_repo)
    _remote(tmp_path, git_repo)
    (git_repo / "dirty").write_text("x")
    with pytest.raises(DispatchGitError, match="dirty"):
        prepare(git_repo, freedom="write_in_repo", branch="feature/x", base="origin/develop")
    (git_repo / "dirty").unlink()
    prepare(git_repo, freedom="write_in_repo", branch="feature/x", base="origin/develop")
    with pytest.raises(DispatchGitError, match="already exists"):
        prepare(git_repo, freedom="publish", branch="feature/x", base="origin/develop")
    for kwargs in (
        {"branch": "-c", "base": "origin/develop"},
        {"branch": "feature/x", "base": "develop"},
        {"branch": "feature/../x", "base": "origin/develop"},
        {"fetch": ["--force"]},
        {"fetch_required": True},
        {"freedom": "read_only", "branch": "feature/x", "base": "origin/develop"},
    ):
        with pytest.raises(DispatchGitError):
            freedom = kwargs.pop("freedom", "write_in_repo")
            preview(freedom=freedom, **kwargs)


def test_fetch_failure_starts_only_when_optional(git_repo: Path):
    _commit(git_repo)
    _git(git_repo, "remote", "add", "down", "http://127.0.0.1:9/no.git")
    before = _head(git_repo)
    soft = prepare(git_repo, freedom="read_only", fetch=["down"])
    assert soft is not None and soft["fetch"][0]["ok"] is False
    assert soft["notices"] and "stale" in soft["notices"][0]
    assert _head(git_repo) == before and _branch(git_repo) in {"main", "master"}
    with pytest.raises(DispatchGitError, match="not started"):
        prepare(git_repo, freedom="read_only", fetch=["down"], fetch_required=True)
    with pytest.raises(DispatchGitError, match="not started"):
        prepare(git_repo, freedom="write_in_repo", branch="feature/x", base="down/develop")


def test_unknown_remote_is_a_recorded_fetch_failure(git_repo: Path):
    _commit(git_repo)
    report = prepare(git_repo, freedom="write_in_repo", fetch=["missing"])
    assert report is not None and report["fetch"][0]["ok"] is False
    assert "missing" in report["notices"][0]


def test_resume_reuses_the_branch_without_recreating_it(git_repo: Path, tmp_path: Path):
    _commit(git_repo)
    _remote(tmp_path, git_repo)
    first = prepare(git_repo, freedom="write_in_repo", branch="feature/x", base="origin/develop")
    (git_repo / "later").write_text("later\n")
    reused = prepare(git_repo, freedom="write_in_repo", branch="feature/x", base="origin/develop", reuse_branch=True)
    assert reused is not None and reused["reused"] is True and reused["created"] is False
    assert reused["base_commit"] == first["base_commit"]
    assert (git_repo / "later").exists()
    _git(git_repo, "checkout", "-q", "main" if _branch_exists(git_repo, "main") else "master")
    with pytest.raises(DispatchGitError, match="will not switch"):
        prepare(git_repo, freedom="write_in_repo", branch="feature/x", base="origin/develop", reuse_branch=True)


def test_codex_read_only_fetch_leaves_the_agent_policy_and_tree(git_repo: Path, tmp_path: Path):
    _commit(git_repo)
    _remote(tmp_path, git_repo)
    before = (_head(git_repo), _branch(git_repo))
    report = prepare(git_repo, freedom="read_only", fetch=True)
    assert report is not None and report["read_only_agent_unchanged"] is True
    assert report["fetch"][0]["ok"] is True
    assert (_head(git_repo), _branch(git_repo)) == before
    assert CodexBackend().enforcement("read_only").local_commit == "not_enforced"
    shown = preview(freedom="read_only", fetch=True)
    assert shown is not None and shown["status"] == "preview" and shown["runs_before_agent"] is True


def _branch_exists(repo: Path, name: str) -> bool:
    return subprocess.run(["git", "show-ref", "--verify", "--quiet", f"refs/heads/{name}"], cwd=repo).returncode == 0
