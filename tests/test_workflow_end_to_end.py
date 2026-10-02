"""Exercise the public CLI, detached supervisor, and real task pipes without model calls."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest


FAKE_CODEX = r'''
import json, os, sys, uuid
from pathlib import Path

def emit(value):
    print(json.dumps(value), flush=True)

prompt = sys.argv[-1]
session = uuid.uuid4().hex
emit({"type": "thread.started", "thread_id": session})
if "quota-fixture" in sys.argv:
    emit({"type": "turn.failed", "error": {"message": "You have hit your usage limit", "code": "usage_limit_reached"}})
    sys.exit(1)
if "workflow decision agent" in prompt:
    context = json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
    edges = context["legal_connections"]
    loop = "E2E_LOOP" in context.get("task", "")
    backward = [e for e in edges if e.get("backward")]
    picked = backward if loop and backward else [e for e in edges if not e.get("backward")]
    if context["node"].get("branch_mode") != "all_matching":
        picked = picked[:1]
    answer = {"action": "continue", "connections": [e["id"] for e in picked], "reason": "Fixture routing decision"}
    if context["node"].get("role") == "implementation":
        answer["task_updates"] = [{"task_id": t["id"], "status": "completed", "reason": "Implementation fixture supplied evidence"} for t in context.get("tasks", [])]
elif "E2E_PLANNING" in prompt:
    answer = {"tasks": [{"id": "work", "title": "Implement the fixture task"}], "summary": "Plan produced"}
elif "E2E_IMPLEMENTATION" in prompt:
    answer = {"summary": "Implementation completed", "completed_task_ids": ["work"], "evidence": "Fixture implementation result"}
else:
    answer = {"summary": "Fixture step completed", "outcome": "approved"}
emit({"type": "item.completed", "item": {"id": "answer", "type": "agent_message", "text": json.dumps(answer)}})
emit({"type": "turn.completed", "usage": {"input_tokens": 1, "output_tokens": 1}})
'''


@pytest.fixture
def workflow_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    home = tmp_path / "home"
    home.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "codex"
    fake.write_text(f"#!{sys.executable}\n" + FAKE_CODEX)
    fake.chmod(0o755)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ["PATH"])
    monkeypatch.delenv("PB_TASK_ID", raising=False)
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1] / "src"))

    def invoke(*args: str, success: bool = True) -> dict:
        result = subprocess.run(
            [sys.executable, "-m", "polybridge.ctl", *args, "--json"],
            capture_output=True, text=True, env=os.environ.copy(), timeout=40,
        )
        if success:
            assert result.returncode == 0, (result.stdout, result.stderr)
        document = json.loads(result.stdout)
        assert document["v"] == 2
        return document.get("result", document)

    return invoke, home


def _definition(name: str, *, loop: bool = False, fallback: bool = False) -> dict:
    candidate = {"backend": "codex"}
    if fallback:
        candidate.update(model="quota-fixture", fallbacks=[{"backend": "codex", "model": "available-fixture"}])
    nodes = [
        {"id": "start", "type": "start"},
        {"id": "plan", "type": "agent", "role": "planning", "instructions": "E2E_PLANNING", "agent": {"backend": "codex"}, "freedom": "read_only"},
        {"id": "implement", "type": "agent", "role": "implementation", "instructions": "E2E_IMPLEMENTATION", "agent": candidate, "session_mode": "fresh"},
        {"id": "review", "type": "agent", "role": "review", "instructions": "Review the fixture", "agent": {"backend": "codex"}, "freedom": "read_only"},
        {"id": "end", "type": "end"},
    ]
    edges = [
        {"id": "begin", "source": "start", "target": "plan"},
        {"id": "planned", "source": "plan", "target": "implement"},
        {"id": "implemented", "source": "implement", "target": "review"},
        {"id": "approved", "source": "review", "target": "end", "condition": "The review approves"},
    ]
    if loop:
        edges.append({"id": "fix", "source": "review", "target": "implement", "condition": "Changes needed", "backward": True})
    return {"name": name, "orchestrator": {"backend": "codex"}, "nodes": nodes, "connections": edges}


def _save(cli, tmp_path: Path, definition: dict) -> None:
    source = tmp_path / "definition.json"
    source.write_text(json.dumps(definition))
    saved = cli("workflow-save", definition["name"], "--definition", str(source), "--expected-revision", "0")
    assert saved["revision"] == 1


def _settled(cli, run_id: str) -> dict:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        run = cli("workflow-status", run_id)
        if run["status"] in {"completed", "failed", "cancelled", "needs_attention", "paused"}:
            return run
        time.sleep(0.05)
    pytest.fail(f"Workflow did not settle: {run}")


def test_cli_supervisor_creates_plan_and_orchestrator_completes_checklist(workflow_cli, tmp_path, git_repo):
    cli, home = workflow_cli
    _save(cli, tmp_path, _definition("e2e-plan"))
    started = cli("workflow-start", "e2e-plan", "--repo", str(git_repo), "--prompt", "Implement fixture")
    assert "task_id" not in started
    run = _settled(cli, started["workflow_run_id"])
    assert run["status"] == "completed", run.get("attention_reason")
    assert len(run["tasks"]) == 1
    assert run["tasks"][0]["status"] == "completed"
    assert run["decisions"]
    assert list((home / ".polybridge" / "tasks").glob("*.events.jsonl"))


def test_cli_loop_limit_stops_after_three_implementation_activations(workflow_cli, tmp_path, git_repo):
    cli, _ = workflow_cli
    _save(cli, tmp_path, _definition("e2e-loop", loop=True))
    started = cli("run", "--workflow", "e2e-loop", "--repo", str(git_repo), "--prompt", "E2E_LOOP")
    run = _settled(cli, started["workflow_run_id"])
    assert run["status"] == "needs_attention"
    attempts = [a for a in run["activations"] if a["node_id"] == "implement" and a["role"] == "node"]
    assert len(attempts) == 3
    assert "limit" in run["attention_reason"].lower()


def test_cli_availability_fallback_keeps_one_activation(workflow_cli, tmp_path, git_repo):
    cli, _ = workflow_cli
    _save(cli, tmp_path, _definition("e2e-fallback", fallback=True))
    started = cli("workflow-start", "e2e-fallback", "--repo", str(git_repo), "--prompt", "Implement fixture")
    run = _settled(cli, started["workflow_run_id"])
    assert run["status"] == "completed", run.get("attention_reason")
    attempts = [a for a in run["activations"] if a["node_id"] == "implement" and a["role"] == "node"]
    assert len(attempts) == 1
    assert [t["candidate"]["model"] for t in attempts[0]["tasks"]] == ["quota-fixture", "available-fixture"]
