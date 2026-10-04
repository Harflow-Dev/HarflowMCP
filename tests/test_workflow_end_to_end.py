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
session = sys.argv[-2] if "resume" in sys.argv else uuid.uuid4().hex
emit({"type": "thread.started", "thread_id": session})
if "quota-fixture" in sys.argv:
    emit({"type": "turn.failed", "error": {"message": "You have hit your usage limit", "code": "usage_limit_reached"}})
    sys.exit(1)
if "workflow orchestrator" in prompt:
    context = json.JSONDecoder().raw_decode(prompt.split("Context:\n", 1)[1])[0]
    choices = context["valid_continuations"]
    loop = "E2E_LOOP" in context.get("original_request", "")
    backward = [c for c in choices if c.get("connection", {}).get("backward")]
    picked = backward if loop and backward else [c for c in choices if not c.get("connection", {}).get("backward")]
    picked = picked if "E2E_PARALLEL" in context.get("original_request", "") else picked[:1]
    stage = context["current_stage"]
    if stage["phase"] == "clarification":
        answer = {"decision_id": context["decision_id"], "action": "answer", "question_id": context["worker_question"]["question_id"], "answer": "Use the established repository conventions", "reason": "The workflow objective supplies the answer"}
    elif stage["node_id"] == "end":
        answer = {"decision_id": context["decision_id"], "action": "complete", "reason": "Fixture complete"}
    elif picked and picked[0].get("attempts_remaining") == 0:
        answer = {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Attempt limit reached", "question": "Grant more attempts?"}
    else:
        next_steps = []
        for choice in picked:
            entry = {"continuation_id": choice["continuation_id"]}
            if choice["requires_prompt"]:
                entry["prompt"] = "Focused fixture assignment for " + choice["node_id"]
                if choice.get("session_mode") == "agent_decides":
                    entry["session_mode"] = "fresh"
                if choice["node_id"] == "implement":
                    entry["assigned_task_ids"] = [t["id"] for t in context.get("checklist", [])]
            next_steps.append(entry)
        answer = {"decision_id": context["decision_id"], "action": "continue", "next": next_steps, "reason": "Fixture routing decision"}
        if stage["node_id"] == "implement" and stage["phase"] == "routing":
            answer["task_updates"] = [{"task_id": t["id"], "status": "completed", "reason": "Implementation fixture supplied evidence"} for t in context.get("checklist", [])]
elif "E2E_ASK" in prompt and "resume" not in sys.argv:
    answer = {"status": "asking", "result": {"question": "Which conventions should I use?", "context": "Need implementation context"}, "evidence": []}
elif "E2E_PLANNING" in prompt:
    answer = {"status": "succeeded", "result": {"tasks": [{"id": "work", "title": "Implement the fixture task"}], "technical_plan": "## Approach\nImplement the fixture behavior and verify its tests.", "summary": "Plan produced"}, "evidence": []}
elif "E2E_IMPLEMENTATION" in prompt:
    answer = {"status": "succeeded", "result": {"summary": "Implementation completed", "completed_task_ids": ["work"]}, "evidence": ["Fixture implementation result"]}
else:
    answer = {"status": "succeeded", "result": {"summary": "Fixture step completed", "verdict": "approved", "findings": []}, "evidence": []}
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
        assert document["v"] == 4
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
        if run["status"] in {"completed", "failed", "cancelled", "needs_attention", "paused", "needs_input"}:
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
    assert run["technical_plan"].startswith("## Approach")
    planning = next(a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "plan")
    assert run["technical_plan_execution_id"] == planning["id"]
    assert planning["node_result"]["result"]["technical_plan"] == run["technical_plan"]
    assert run["decisions"]
    assert list((home / ".polybridge" / "tasks").glob("*.events.jsonl"))


def test_cli_loop_limit_stops_after_three_implementation_activations(workflow_cli, tmp_path, git_repo):
    cli, _ = workflow_cli
    _save(cli, tmp_path, _definition("e2e-loop", loop=True))
    started = cli("run", "--workflow", "e2e-loop", "--repo", str(git_repo), "--prompt", "E2E_LOOP")
    run = _settled(cli, started["workflow_run_id"])
    assert run["status"] == "needs_input"
    attempts = [a for a in run["activations"] if a["node_id"] == "implement" and a["role"] == "node"]
    assert len(attempts) == 3
    assert "Grant more attempts" in run["input_question"]


def test_cli_availability_fallback_keeps_one_activation(workflow_cli, tmp_path, git_repo):
    cli, _ = workflow_cli
    _save(cli, tmp_path, _definition("e2e-fallback", fallback=True))
    started = cli("workflow-start", "e2e-fallback", "--repo", str(git_repo), "--prompt", "Implement fixture")
    run = _settled(cli, started["workflow_run_id"])
    assert run["status"] == "completed", run.get("attention_reason")
    attempts = [a for a in run["activations"] if a["node_id"] == "implement" and a["role"] == "node"]
    assert len(attempts) == 1
    assert [t["candidate"]["model"] for t in attempts[0]["tasks"]] == ["quota-fixture", "available-fixture"]


def _question_definition(name: str, *, parallel: bool = False) -> dict:
    nodes = [{"id": "start", "type": "start"}, {"id": "worker", "type": "agent", "role": "task", "instructions": "E2E_ASK", "freedom": "read_only", "agent": {"backend": "codex"}}, {"id": "end", "type": "end"}]
    connections = [{"id": "begin", "source": "start", "target": "worker"}, {"id": "finish", "source": "worker", "target": "end"}]
    if parallel:
        nodes.extend([{"id": "sibling", "type": "agent", "role": "task", "instructions": "E2E_ASK", "freedom": "read_only", "agent": {"backend": "codex"}}, {"id": "converge", "type": "agent", "role": "review", "instructions": "Review both workers", "freedom": "read_only", "agent": {"backend": "codex"}}])
        connections = [{"id": "begin", "source": "start", "target": "worker"}, {"id": "split", "source": "start", "target": "sibling"}, {"id": "join1", "source": "worker", "target": "converge"}, {"id": "join2", "source": "sibling", "target": "converge"}, {"id": "finish", "source": "converge", "target": "end"}]
    return {"name": name, "orchestrator": {"backend": "codex"}, "nodes": nodes, "connections": connections}


@pytest.mark.parametrize("parallel", [False, True])
def test_cli_worker_questions_resume_same_execution_before_convergence(workflow_cli, tmp_path, git_repo, parallel):
    cli, _ = workflow_cli
    _save(cli, tmp_path, _question_definition("e2e-questions", parallel=parallel))
    started = cli("workflow-start", "e2e-questions", "--repo", str(git_repo), "--prompt", "E2E_PARALLEL" if parallel else "Answer worker questions")
    run = _settled(cli, started["workflow_run_id"])
    assert run["status"] == "completed", run.get("failure_reason", run.get("attention_reason"))
    workers = [a for a in run["activations"] if a["role"] == "node" and a["node_id"] in {"worker", "sibling"}]
    assert len(workers) == (2 if parallel else 1)
    for worker in workers:
        assert worker["status"] == "completed"
        assert len(worker["questions"]) == 1
        assert worker["questions"][0]["status"] == "answered"
        assert len(worker["tasks"]) == 2
        assert worker["tasks"][0]["result"]["session_id"] == worker["tasks"][1]["result"]["session_id"]
        assert worker["tasks"][1]["assignment_prompt"] == "Use the established repository conventions"
    if parallel:
        assert len([a for a in run["activations"] if a["role"] == "node" and a["node_id"] == "converge"]) == 1
