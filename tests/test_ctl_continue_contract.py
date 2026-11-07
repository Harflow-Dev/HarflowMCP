"""`harflow-ctl continue` public contract: capability, launch fixtures and refusal documents.

The detached owner is replaced by an injected outcome; everything else runs through the
real ctl dispatch on a temporary state root. No agent is spawned and nothing is networked.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from harflow_mcp import ctl, detached, state_ownership, store
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import default_log_dir
from harflow_mcp.wire_contracts import CAPABILITIES

ROOT = Path(__file__).parents[1]
BUNDLE = ROOT / "contracts/v1"
FULL_ID = "11111111-aaaa-4aaa-8aaa-111111111111"
OTHER_ID = "11111111-bbbb-4bbb-8bbb-222222222222"


def _canonical():
    spec = importlib.util.spec_from_file_location("handshake_fixtures", ROOT / "scripts/generate-handshake-fixtures.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.canonical


def _record(task_id: str, **overrides) -> store.TaskRecord:
    now = datetime.now(timezone.utc).isoformat()
    base = {"task_id": task_id, "backend": "claude", "session_id": "s-" + task_id, "markers": ["s-" + task_id],
            "repo_path": "/tmp/repo", "started_at": now, "status": "completed", "exit_code": 0, "finished_at": now,
            "title": "title of " + task_id,
            "owner": {"pid": 4242, "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}}
    return store.TaskRecord(**(base | overrides))


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        pass
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    return root


def _run(capsys: pytest.CaptureFixture, *argv: str) -> tuple[int, dict]:
    code = ctl.main(["continue", *argv, "--json"])
    return code, json.loads(capsys.readouterr().out)


def test_capability_is_advertised_and_fixtures_declare_it():
    assert "task-continue.v1" in CAPABILITIES
    version = json.loads((BUNDLE / "version-info.json").read_text())
    assert "task-continue.v1" in version["capabilities"]
    manifest = json.loads((BUNDLE / "manifest.json").read_text())
    for label, exit_code in (("accepted", 0), ("refused", 1), ("unknown", 3)):
        case = manifest["cases"][f"ctl-launch-continue-{label}.json"]
        assert case["required_capabilities"] == ["task-continue.v1"]
        assert case["exit_code"] == exit_code
    missing = manifest["cases"]["invalid-s05-missing-task-continue.json"]
    assert missing["required_capabilities"] == ["task-continue.v1"] and missing["valid"] is False
    document = json.loads((BUNDLE / "invalid-s05-missing-task-continue.json").read_text())
    assert "task-continue.v1" not in document["capabilities"]


@pytest.mark.parametrize("label", ["accepted", "refused", "unknown"])
def test_continue_fixtures_are_byte_identical_to_run_and_resume(label):
    continued = (BUNDLE / f"ctl-launch-continue-{label}.json").read_bytes()
    assert continued == (BUNDLE / f"ctl-launch-run-{label}.json").read_bytes()
    assert continued == (BUNDLE / f"ctl-launch-resume-{label}.json").read_bytes()


@pytest.mark.parametrize("label, kind, payload, exit_code", [
    ("accepted", "task", {"task_id": "task-fixture"}, 0),
    ("refused", "error", {"code": "invalid_params", "message": "fixture refusal"}, 1),
    ("unknown", "unknown", {"message": "fixture: owner did not report; a task may exist"}, 3),
])
def test_continue_dispatch_matches_launch_fixture(state, monkeypatch, capsys, label, kind, payload, exit_code):
    store.write(default_log_dir(), _record(FULL_ID))
    seen = []

    def fake(action, **kwargs):
        seen.append(action)
        return detached.Outcome(kind, payload, 123)

    monkeypatch.setattr(detached, "run_detached", fake)
    code = ctl.main(["continue", FULL_ID, "--json"])
    out = capsys.readouterr().out
    assert code == exit_code and len(seen) == 1
    assert _canonical()(json.loads(out)) == (BUNDLE / f"ctl-launch-continue-{label}.json").read_bytes()


def test_precondition_value_error_reports_start_failed(state, monkeypatch, capsys):
    # A source that has not settled is a real precondition refusal inside continue_task.
    store.write(default_log_dir(), _record(FULL_ID, status="running", exit_code=None, finished_at=None))

    def fake(action, *, registry_factory, **kwargs):
        try:
            asyncio.run(action(registry_factory()))
        except BaseException as exc:
            return detached.Outcome("error", detached.error_doc(exc), 123)
        raise AssertionError("continue was expected to be refused")

    monkeypatch.setattr(detached, "run_detached", fake)
    code, document = _run(capsys, FULL_ID)
    assert code == 1
    error = document["error"]
    assert error["code"] == "start_failed"
    assert error["message"].startswith("ValueError: ")
    assert "data" not in error


def _forbid_launch(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("refusals must happen before the detached owner is started")
    monkeypatch.setattr(detached, "run_detached", boom)


def test_invalid_task_id_is_refused_before_fork(state, monkeypatch, capsys):
    _forbid_launch(monkeypatch)
    code, document = _run(capsys, "bad/id")
    assert code == 1 and document["error"]["code"] == "invalid_task_id"


def test_unknown_task_is_refused_before_fork(state, monkeypatch, capsys):
    _forbid_launch(monkeypatch)
    code, document = _run(capsys, "no-such-prefix-at-all")
    error = document["error"]
    assert code == 1 and error["code"] == "invalid_params"
    assert error["data"]["code"] == "unknown_task" and "input" in error["data"]


def test_ambiguous_prefix_is_refused_with_candidates_before_fork(state, monkeypatch, capsys):
    store.write(default_log_dir(), _record(FULL_ID))
    store.write(default_log_dir(), _record(OTHER_ID))
    _forbid_launch(monkeypatch)
    code, document = _run(capsys, "11111111")
    error = document["error"]
    assert code == 1 and error["code"] == "ambiguous_task_id"
    assert {candidate if isinstance(candidate, str) else candidate.get("task_id") for candidate in error["data"]["candidates"]} >= {FULL_ID, OTHER_ID}


def test_undecidable_caller_detection_is_refused_before_fork(state, monkeypatch, capsys):
    from harflow_mcp import lineage
    store.write(default_log_dir(), _record(FULL_ID))
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: None)
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "detection is undecidable"))
    _forbid_launch(monkeypatch)
    code, document = _run(capsys, FULL_ID)
    error = document["error"]
    assert code == 1 and error["code"] == "nested_dispatch_refused"
    assert "data" not in error


def test_state_failure_reports_an_exit_one_envelope(state, monkeypatch, capsys):
    store.write(default_log_dir(), _record(FULL_ID))
    _forbid_launch(monkeypatch)

    def broken(*args, **kwargs):
        raise state_ownership.StateOwnershipError("fixture state failure")

    monkeypatch.setattr(ctl, "_resolve_task_arg", broken)
    code, document = _run(capsys, FULL_ID)
    assert code == 1
    error = document["error"]
    assert error["code"] == "state_incompatible"
    assert "fixture state failure" in error["message"]
    assert error["data"] == {}


def test_unconfirmed_worker_variable_is_caller_undecidable(state, monkeypatch, capsys):
    store.write(default_log_dir(), _record(FULL_ID))
    _forbid_launch(monkeypatch)
    monkeypatch.setenv("HARFLOW_TASK_ID", "no-live-parent")
    code, document = _run(capsys, FULL_ID)
    assert code == 1 and document["error"]["code"] == "caller_undecidable"
