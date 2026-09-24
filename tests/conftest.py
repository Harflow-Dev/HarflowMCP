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
def no_monitor_app(monkeypatch: pytest.MonkeyPatch):
    """`PB_OPEN_MONITOR=0`: a root task must never open the real Monitor app from a test (A4.3).
    Tests of that behaviour inject a launcher and lift this themselves."""
    monkeypatch.setenv("PB_OPEN_MONITOR", "0")


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


# A synthetic owner identity that `identities` reports alive. Tests that need a *live* owning server
# use this instead of `identity.own_identity()`, whose verdict depends on a working `ps`.
ALIVE_OWNER = {"pid": 424_242, "start_time": "Wed Jan  1 00:00:00 2026", "markers": []}


class IdentityStub:
    """Stands in for `identity.identity_check`: verdicts keyed by pid, `undecidable` otherwise —
    the same answer a sandbox with no working `ps` gives, so a test never passes by accident of
    the environment."""

    def __init__(self) -> None:
        self.verdicts: dict[int, str] = {ALIVE_OWNER["pid"]: "alive"}

    def alive(self, pid: int) -> None:
        self.verdicts[pid] = "alive"

    def dead(self, pid: int) -> None:
        self.verdicts[pid] = "dead"

    def __call__(self, ident) -> str:
        if not isinstance(ident, dict) or ident.get("pid") is None:
            return "undecidable"
        return self.verdicts.get(ident["pid"], "undecidable")


@pytest.fixture
def identities(monkeypatch: pytest.MonkeyPatch) -> IdentityStub:
    """Replace `identity.identity_check` (looked up at call time by `inbox`, `store`, `control`)
    with an `IdentityStub`, so live-input send checks do not depend on `ps`."""
    from polybridge import identity

    stub = IdentityStub()
    monkeypatch.setattr(identity, "identity_check", stub)
    return stub
