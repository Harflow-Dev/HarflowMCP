from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from polybridge import server
from polybridge.tasks import TaskRegistry


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Give each test a fresh registry writing its stream logs under tmp_path."""
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=tmp_path / "streams"))
    yield
    monkeypatch.setattr(server, "_registry", None)


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    return repo
