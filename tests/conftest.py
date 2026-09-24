from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from polybridge import lineage, server
from polybridge.tasks import TaskRegistry


@pytest.fixture(autouse=True)
def isolated_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Give each test a fresh registry writing its stream logs under tmp_path."""
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=tmp_path / "streams"))
    yield
    monkeypatch.setattr(server, "_registry", None)


@pytest.fixture(autouse=True)
def no_caller_detected(monkeypatch: pytest.MonkeyPatch):
    """Neutralise best-effort caller detection everywhere by default.

    `TaskRegistry.start`/`resume`/`resume_record` look up `lineage.detect_caller` through the
    module attribute at call time (see `TaskRegistry._detect_caller`), specifically so this works:
    without it, caller detection would walk the *real* process tree during every test that spawns
    a task, occasionally finding an unrelated ancestor and applying the nested-dispatch caps to a
    test that never asked for them. Tests that exercise `detect_caller` itself
    (`tests/test_lineage.py`) grab the real function at import time, before this fixture can touch
    it, so they are unaffected.
    """
    monkeypatch.setattr(lineage, "detect_caller", lambda *args, **kwargs: None)


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    return repo
