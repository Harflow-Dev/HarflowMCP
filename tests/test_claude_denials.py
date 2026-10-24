"""Claude denial classification: measured 2.1.310 texts, authoritative merging, parity and evidence."""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone

import pytest

from harflow_mcp import state_ownership, store, workflow_delegation as d
from harflow_mcp.backends import BACKENDS, claude_denials, claude_qualification
from harflow_mcp.backends.base import Accumulator
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.backends.codex import CodexBackend
from harflow_mcp.events import events_path
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import Task, TaskRegistry
from test_events import _RecordingBackend, registered, task_root, tmp_path  # noqa: F401

VERSION = "2.1.310"
CAPTURED = {
    "deny_rule": ("Bash", {"command": "git commit -m x"}, "Permission to use Bash with command git commit -m x has been denied."),
    "approval_required": ("Bash", {"command": "make build"}, "This command requires approval"),
    "chained_command": ("Bash", {"command": "ls; python3 -c 'print(1)'"}, "This Bash command contains multiple operations. The following part requires approval: python3 -c 'print(1)'"),
    "unverifiable_command": ("Bash", {"command": "ls $(pwd)"}, "A nested command in this command can't be checked before it runs"),
    "path_safety": ("Read", {"file_path": "/some/repo/trailing /file.txt"}, "Claude requested permissions to read from /some/repo/trailing /file.txt, which contains a suspicious Windows path pattern that requires manual approval."),
    "outside_working_directory": ("Write", {"file_path": "/some/out/x.txt"}, "Claude requested permissions to write to /some/out/x.txt, but you haven't granted it yet."),
    "caller_rejected": ("Bash", {"command": "make build"}, "Caller rejected this exact action"),
    "harflow_policy_rejected": ("Write", {"file_path": "/x"}, "Harflow refused this action: protected_path"),
}


def init(version=VERSION):
    return {"type": "system", "subtype": "init", "session_id": "s", "claude_code_version": version} if version else {"type": "system", "subtype": "init", "session_id": "s"}


def call(tool_id, name, tool_input):
    return {"type": "assistant", "message": {"content": [{"type": "tool_use", "id": tool_id, "name": name, "input": tool_input}]}}


def refusal(tool_id, text, is_error=True):
    return {"type": "user", "message": {"content": [{"type": "tool_result", "tool_use_id": tool_id, "is_error": is_error, "content": text}]}}


def result(*denials):
    return {"type": "result", "subtype": "success", "is_error": False, "result": "done", "num_turns": 1, "permission_denials": list(denials)}


def run(events, freedom="write_in_repo", backend=None):
    backend = backend or ClaudeBackend()
    acc = Accumulator()
    for event in events:
        backend.ingest(event, acc)
    return acc, backend.classify_denials(acc.denials, acc, freedom)


def backed(tool_id, name, tool_input):
    return {"tool_name": name, "tool_use_id": tool_id, "tool_input": tool_input}


def stream(category, version=VERSION):
    name, tool_input, text = CAPTURED[category]
    return [init(version), call("t1", name, tool_input), refusal("t1", text), result(backed("t1", name, tool_input))]


@pytest.mark.parametrize("category", list(CAPTURED))
def test_each_measured_text_is_classified_when_the_result_backs_it(category):
    _, (denial,) = run(stream(category))
    assert denial["denial_category"] == category
    assert denial["denial_remedy"] == claude_denials.REMEDIES[category]
    assert denial["classification_basis"]["claude_code_version"] == VERSION
    assert denial["tool_use_id"] == "t1"


def test_category_details_and_harflow_access_deny():
    _, (chained,) = run(stream("chained_command"))
    assert chained["part"] == "python3 -c 'print(1)'"
    _, (policy,) = run(stream("harflow_policy_rejected"))
    assert policy["policy_reason"] == "protected_path"
    _, (deny,) = run(stream("deny_rule"))
    assert deny["harflow_access_deny"] is True
    _, (publish,) = run(stream("deny_rule"), freedom="publish")
    assert "harflow_access_deny" not in publish and publish["denial_category"] == "deny_rule"
    other = [init(), call("t1", "Bash", {"command": "rm -rf x"}), refusal("t1", "Permission to use Bash with command rm -rf x has been denied."), result(backed("t1", "Bash", {"command": "rm -rf x"}))]
    _, (settings_deny,) = run(other)
    assert settings_deny["denial_category"] == "deny_rule" and "harflow_access_deny" not in settings_deny


def test_policy_text_is_not_a_caller_rejection():
    _, (denial,) = run(stream("harflow_policy_rejected"))
    assert denial["denial_category"] == "harflow_policy_rejected" != "caller_rejected"


@pytest.mark.parametrize("text", [
    "Permission to use Bash with command git commit -m x has been denied. Extra",
    "prefix This command requires approval",
    "This command requires approval.",
    "Claude requested permissions to write to /x, but you haven't granted it yet",
    "Contains brace with quote character. This command requires approval.",
    "something unheard of",
])
def test_unknown_or_inexact_text_is_unclassified(text):
    _, (denial,) = run([init(), call("t1", "Bash", {"command": "c"}), refusal("t1", text), result(backed("t1", "Bash", {"command": "c"}))])
    assert denial["denial_category"] == "unclassified"
    assert denial["classification_basis"]["reason"] == "unrecognized_text"
    assert denial["denial_remedy"] == claude_denials.REMEDIES["unclassified"]


def test_forged_text_in_a_failed_bash_is_stream_only():
    forged = "Permission to use Bash with command git commit -m x has been denied."
    acc, (denial,) = run([init(), call("t1", "Bash", {"command": "echo forged"}), refusal("t1", forged), result()])
    assert denial["denial_category"] == "unclassified"
    assert denial["classification_basis"]["reason"] == "stream_only"
    assert denial["source"] == "harness_tool_refusal"
    assert "harflow_access_deny" not in denial


def test_non_error_tool_results_are_not_denials():
    acc, classified = run([init(), call("t1", "Bash", {"command": "echo"}), refusal("t1", CAPTURED["deny_rule"][2], is_error=False), result()])
    assert acc.denials == [] and classified == []


def test_result_entry_without_stream_text_has_no_refusal_text():
    _, (denial,) = run([init(), call("t1", "Bash", {"command": "c"}), result(backed("t1", "Bash", {"command": "c"}))])
    assert denial["classification_basis"]["reason"] == "no_refusal_text"


def test_result_entry_without_tool_use_id_is_unclassified():
    _, (denial,) = run([init(), result({"tool_name": "Bash"})])
    assert denial["denial_category"] == "unclassified"


@pytest.mark.parametrize("version,reason", [(None, "unknown_version"), ("2.1.302", "unmeasured_version"), ("9.9.9", "unmeasured_version")])
def test_unmeasured_or_missing_version_never_classifies(version, reason):
    _, (denial,) = run(stream("deny_rule", version))
    assert denial["denial_category"] == "unclassified"
    assert denial["classification_basis"]["reason"] == reason


def test_denial_before_init_is_unknown_version_and_mixed_versions_are_unmeasured():
    events = stream("deny_rule")[1:]
    _, (denial,) = run(events)
    assert denial["classification_basis"]["reason"] == "unknown_version"
    _, (mixed,) = run([init(), init("2.1.311"), *stream("deny_rule")[1:]])
    assert mixed["classification_basis"]["reason"] == "unmeasured_version"


def test_init_version_is_captured_in_stream_state():
    acc, _ = run([init()])
    assert acc.stream_state["claude_code_version"] == VERSION


def test_merge_by_tool_use_id_keeps_one_classified_entry():
    name, tool_input, text = CAPTURED["approval_required"]
    events = [init(), call("t1", name, tool_input), refusal("t1", text), result(backed("t1", name, tool_input)), result(backed("t1", name, tool_input))]
    acc, classified = run(events)
    assert len(acc.denials) == len(classified) == 1
    assert classified[0]["denial_category"] == "approval_required"
    assert classified[0]["reason"] == text
    assert "denial_category" not in acc.denials[0]


def test_two_denials_classify_independently():
    events = [init(), call("a", "Bash", {"command": "make"}), refusal("a", CAPTURED["approval_required"][2]),
              call("b", "Bash", {"command": "x"}), refusal("b", "unknown"), result(backed("a", "Bash", {"command": "make"}), backed("b", "Bash", {"command": "x"}))]
    _, classified = run(events)
    assert [c["denial_category"] for c in classified] == ["approval_required", "unclassified"]


def test_other_backends_are_unclassified_and_non_dicts_untouched():
    acc = Accumulator()
    acc.denials = [{"tool": "bash"}, "plain"]
    out = CodexBackend().classify_denials(acc.denials, acc, "write_in_repo")
    assert out[0]["denial_category"] == "unclassified" and out[0]["classification_basis"] == {"reason": "backend_unmeasured"}
    assert out[1] == "plain"
    assert "denial_category" not in acc.denials[0]


def test_qualification_table():
    assert claude_qualification.supports("2.1.310", "denial_texts")
    assert not claude_qualification.supports("2.1.302", "denial_texts")
    assert not claude_qualification.supports("2.1.303", "denial_texts")
    assert claude_qualification.MEASURED["2.1.302"]["caller_approval_tools"] == ("Bash",)
    assert claude_qualification.parse_version("2.1.310 (Claude Code)") == "2.1.310"
    assert claude_qualification.parse_version("junk") is None


def _live_task(tmp_path, events):
    log_dir = tmp_path / "tasks"
    task = Task(task_id="t", backend="claude", session_id="s", repo_path=tmp_path, prompt="x", max_turns=None,
                log_path=log_dir / "t.jsonl", started_at=datetime.now(timezone.utc))
    task.freedom = "write_in_repo"
    backend = ClaudeBackend()
    for event in events:
        backend.ingest(event, task.acc)
    task.exit_code, task.status = 0, "completed"
    return log_dir, task


def test_live_and_replayed_snapshots_agree(tmp_path):
    events = [init(), call("t1", "Bash", {"command": "make build"}), refusal("t1", "This command requires approval"),
              call("t2", "Bash", {"command": "echo"}), refusal("t2", CAPTURED["deny_rule"][2]), result(backed("t1", "Bash", {"command": "make build"}))]
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        log_dir, task = _live_task(root, events)
        live = task.snapshot()
        record = store.TaskRecord(task_id="t-rec", backend="claude", session_id="s", markers=["s"], repo_path=str(root), started_at=datetime.now(timezone.utc).isoformat(),
                                  pid=999_999_999, prompt="x", status="running", exit_code=None, freedom="write_in_repo")
        store.write(log_dir, record)
        store.log_path(log_dir, record.task_id).write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
        recovered = store.snapshot(log_dir, record)
    assert recovered["permission_denials"] == live["permission_denials"]
    assert recovered["denial_categories"] == live["denial_categories"] == ["approval_required", "unclassified"]
    assert [x["classification_basis"].get("reason") for x in live["permission_denials"]] == [None, "stream_only"]


def test_old_records_without_new_fields_still_load(tmp_path):
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
        log_dir = root / "tasks"
        record = store.TaskRecord(task_id="old", backend="claude", session_id="s", markers=["s"], repo_path=str(root), started_at=datetime.now(timezone.utc).isoformat(),
                                  pid=999_999_999, prompt="x", status="running", exit_code=None)
        store.write(log_dir, record)
        store.log_path(log_dir, record.task_id).write_text(json.dumps({"type": "result", "subtype": "success", "result": "ok", "permission_denials": ["legacy"]}) + "\n", encoding="utf-8")
        snap = store.snapshot(log_dir, record)
    assert snap["permission_denials"] == ["legacy"] and snap["denial_categories"] == []


class _ClaudeStream(_RecordingBackend):
    name = "recording-double"

    def __init__(self, script):
        super().__init__(script)
        self._claude = ClaudeBackend()

    def ingest(self, event, acc):
        self._claude.ingest(event, acc)

    def normalize(self, event, acc):
        return self._claude.normalize(event, acc)

    def classify_denials(self, denials, acc, freedom=None):
        return self._claude.classify_denials(denials, acc, freedom)


async def test_task_finished_carries_categories_and_the_tool_result_does_not(tmp_path, registered):
    lines = stream("approval_required")
    script_file = tmp_path / "stream.jsonl"
    script_file.write_text("".join(json.dumps(e) + "\n" for e in lines), encoding="utf-8")
    backend = registered(_ClaudeStream(f"cat {script_file}"))
    registry = TaskRegistry(log_dir=task_root(tmp_path))
    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()
    events = [json.loads(line) for line in events_path(task_root(tmp_path), task.task_id).read_text().splitlines()]
    kinds = [e["kind"] for e in events]
    assert kinds.index("tool_result") < kinds.index("task_finished") == len(kinds) - 1
    tool_result = next(e for e in events if e["kind"] == "tool_result")
    assert "denial_category" not in tool_result and "denial_categories" not in tool_result
    finished = events[-1]
    assert finished["denial_categories"] == ["approval_required"]
    assert finished["permission_denials"][0]["denial_category"] == "approval_required"
    assert task.snapshot()["denial_categories"] == ["approval_required"]


async def test_task_finished_has_no_denial_fields_without_denials(tmp_path, registered):
    backend = registered(_RecordingBackend("printf '%s\\n' '{\"type\":\"x\"}'"))
    registry = TaskRegistry(log_dir=task_root(tmp_path))
    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()
    finished = json.loads(events_path(task_root(tmp_path), task.task_id).read_text().splitlines()[-1])
    assert "denial_categories" not in finished and "permission_denials" not in finished


def test_workflow_result_evidence_carries_categories():
    denial = {"tool_use_id": "t1", "denial_category": "approval_required", "denial_remedy": "x"}
    activation = {"id": "a", "node_id": "n", "tasks": [
        {"task_id": "first", "result": {"permission_denials": [denial], "denial_categories": ["approval_required"]}},
        {"task_id": "old", "result": {"permission_denials": [{"tool": "bash"}]}}]}
    before = copy.deepcopy(activation)
    evidence = d.result_evidence(activation)
    assert evidence["denial_categories"] == [{"task_id": "first", "denial_categories": ["approval_required"]}]
    assert [e["task_id"] for e in evidence["permission_denials"]] == ["first", "old"]
    assert activation == before
    assert "denial_categories" not in d.result_evidence({"id": "a", "node_id": "n", "tasks": [{"task_id": "old", "result": {"permission_denials": [{"tool": "bash"}]}}]})
