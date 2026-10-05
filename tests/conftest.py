from __future__ import annotations

import os
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
    # The fail-closed variant takeover uses: "positively no caller" by default. Tests of the
    # undecidable cases stub it themselves or restore the real one.
    monkeypatch.setattr(
        lineage, "detect_caller_detail", lambda *args, **kwargs: lineage.Detection(None)
    )
    # A suite run from inside a polybridge-managed worker inherits `PB_TASK_ID`; the
    # registry would then (correctly) refuse mutations as an unverifiable managed
    # caller. CI runs without it, so clear it to keep the same isolation everywhere.
    monkeypatch.delenv("PB_TASK_ID", raising=False)


@pytest.fixture
def git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)
    return repo


# The server refuses a backend whose CLI is not on PATH before it validates anything else, so a
# test of those later checks needs *a* binary there — never the real one, which a CI runner lacks
# and a developer machine may or may not have. Nothing these tests reach runs the binary; if one ever
# does, the stand-in fails loudly instead of pretending to be an agent.
FAKE_CLI = """#!/bin/sh
echo "fake $(basename "$0") stand-in from tests/conftest.py was invoked: $*" >&2
exit 97
"""


@pytest.fixture
def fake_backend_clis(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    from polybridge.backends import BACKENDS

    bin_dir = tmp_path / "fake-bin"
    bin_dir.mkdir()
    for backend in BACKENDS.values():
        fake = bin_dir / backend.binary
        fake.write_text(FAKE_CLI)
        fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    return bin_dir


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


@pytest.fixture(autouse=True)
def only_v1_event_kinds(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch):
    """Every event kind any test causes to be written must be in `events.EVENT_KINDS`.

    `EventLog.write` already raises for an unknown kind, but every call site guards its write (a
    broken event log must never change an outcome), so that raise alone would be swallowed. This
    records each attempted kind and fails the test that produced an unlisted one.
    """
    from polybridge import events

    attempted: set[str] = set()
    real_write = events.EventLog.write

    def recording_write(self, kind, fields, **kwargs):
        attempted.add(kind)
        return real_write(self, kind, fields, **kwargs)

    monkeypatch.setattr(events.EventLog, "write", recording_write)
    yield
    if request.node.get_closest_marker("allow_unknown_event_kinds"):
        return
    unknown = attempted - events.EVENT_KINDS
    assert not unknown, f"event kinds outside events.EVENT_KINDS were written: {sorted(unknown)}"
