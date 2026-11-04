"""`harflow-ctl workflow-inspect --view activity` on a node execution that has not settled."""
from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from harflow_mcp import ctl, server, takeover, workflows
from harflow_mcp.events import events_path
from harflow_mcp.tasks import TaskRegistry
from harflow_mcp.wire_contracts import CAPABILITIES, PRODUCT_VERSION
from harflow_mcp.workflow_inspection import inspect_request


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("HARFLOW_TASK_ID", raising=False)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    return tmp_path


def event_line(task_id: str, seq: int) -> bytes:
    document = {"product": "harflow", "format": "harflow.event", "v": 1, "product_version": PRODUCT_VERSION,
                "seq": seq, "kind": "assistant_text", "observed_at": "2027-01-01T00:00:00+00:00",
                "task_id": task_id, "text": f"text {seq}"}
    return (json.dumps(document) + "\n").encode()


def build(tmp_path: Path, activations: list[dict], *, contract: str = "delegation"):
    storage = workflows.WorkflowStore()
    definition = storage.save("live-graph", {"nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
                                             "connections": [{"id": "finish", "source": "start", "target": "end"}]})
    run_id = storage.create_run(definition, "synthetic assignment", tmp_path)["workflow_run_id"]

    def reserve(value):
        value.update(status="running", execution_contract=contract, activations=activations)
    storage.update_run(run_id, reserve, "synthetic_authority")
    (storage.root / "tasks").mkdir(exist_ok=True)
    return storage, run_id


def activation(identifier: str, status: str, *tasks: dict, **extra) -> dict:
    return {"id": identifier, "node_id": "node-" + identifier, "role": "node", "status": status, "created_at": 0.0,
            "tasks": list(tasks), **extra}


def log(storage, task_id: str, count: int) -> None:
    events_path(storage.root / "tasks", task_id).write_bytes(b"".join(event_line(task_id, i) for i in range(count)))


def inspect(capsys, run_id: str, execution_id: str, *args: str, expect: int = 0):
    code = ctl.main(["workflow-inspect", run_id, execution_id, "--view", "activity", *args, "--json"])
    document = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert code == expect, document
    return document["result"] if expect == 0 else document["error"]


def test_capability_is_declared():
    assert "workflow-live-activity.v1" in CAPABILITIES


@pytest.mark.parametrize("status", ["running", "starting", "uncertain"])
def test_direct_caller_reads_a_live_attempt(tmp_path, capsys, status):
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": status})])
    log(storage, "att-1", 3)
    result = inspect(capsys, run_id, "exec")
    assert [event["seq"] for event in result["events"]] == [0, 1, 2]
    assert (result["task_id"], result["attempt_status"], result["settled"], result["activity_state"]) == ("att-1", status, False, "available")
    assert "reason" not in result
    tail = inspect(capsys, run_id, "exec", "--after-seq", "1")
    assert [event["seq"] for event in tail["events"]] == [2]


def test_reserved_attempt_and_missing_log_are_not_started(tmp_path, capsys):
    storage, run_id = build(tmp_path, [activation("reserved", "running", {"task_id": "att-r", "status": "reserved"}),
                                      activation("nolog", "running", {"task_id": "att-n", "status": "running"})])
    log(storage, "att-r", 2)  # a reserved attempt is not started even if a stray log exists
    for execution_id, status in (("reserved", "reserved"), ("nolog", "running")):
        result = inspect(capsys, run_id, execution_id)
        assert result["events"] == [] and result["has_more"] is False
        assert (result["activity_state"], result["attempt_status"], result["settled"]) == ("not_started", status, False)


def test_execution_without_an_attempt_is_not_started(tmp_path, capsys):
    _, run_id = build(tmp_path, [activation("empty", "running")])
    result = inspect(capsys, run_id, "empty")
    assert result["task_id"] is None and result["events"] == []
    assert (result["activity_state"], result["attempt_status"], result["settled"]) == ("not_started", None, False)


def test_unsafe_log_is_unavailable_with_a_reason(tmp_path, capsys):
    storage, run_id = build(tmp_path, [activation("link", "running", {"task_id": "att-l", "status": "running"}),
                                      activation("dir", "running", {"task_id": "att-d", "status": "running"})])
    target = tmp_path / "elsewhere.jsonl"
    target.write_bytes(event_line("att-l", 0))
    events_path(storage.root / "tasks", "att-l").symlink_to(target)
    events_path(storage.root / "tasks", "att-d").mkdir()
    for execution_id in ("link", "dir"):
        result = inspect(capsys, run_id, execution_id)
        assert result["events"] == [] and result["activity_state"] == "unavailable"
        assert result["reason"] in {"log_unreadable", "log_not_regular_file"}


def test_settled_execution_keeps_its_requirements_and_gains_the_state(tmp_path, capsys):
    settled = activation("done", "completed", {"task_id": "att-s", "status": "completed", "result": {"summary": "ok"}},
                         node_result={"status": "succeeded", "summary": "done"})
    unnormalized = activation("bare", "completed", {"task_id": "att-b", "status": "completed"})
    storage, run_id = build(tmp_path, [settled, unnormalized])
    log(storage, "att-s", 2)
    result = inspect(capsys, run_id, "done")
    assert (result["settled"], result["activity_state"], result["attempt_status"]) == (True, "available", "completed")
    assert "final normalized result" in inspect(capsys, run_id, "bare", expect=1)["message"]


def test_result_view_is_settled_only_for_a_direct_caller(tmp_path, capsys):
    _, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    assert ctl.main(["workflow-inspect", run_id, "exec", "--view", "result", "--json"]) == 1
    assert "fully settled" in json.loads(capsys.readouterr().out)["error"]["message"]


@pytest.mark.parametrize("refusal", [("agent_caller", "an agent"), ("caller_undecidable", "cannot tell")])
def test_other_callers_keep_the_settled_only_read(tmp_path, capsys, monkeypatch, refusal):
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    log(storage, "att-1", 1)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: refusal)
    assert "fully settled" in inspect(capsys, run_id, "exec", expect=1)["message"]


def test_managed_view_keeps_the_settled_only_read_even_for_a_direct_caller(tmp_path, capsys, monkeypatch):
    from harflow_mcp import workflow_inspection
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"}),
                                      activation("decision", "running", role="orchestrator")])
    log(storage, "att-1", 1)
    managed = ({"role": "orchestrator", "activation_id": "decision"}, storage.get_run(run_id))
    monkeypatch.setattr(workflow_inspection, "managed_reader", lambda log_dir: managed)
    # caller_refusal is permissive (autouse), so only the managed view can refuse.
    assert "fully settled" in inspect(capsys, run_id, "exec", expect=1)["message"]


def test_ordinary_agent_with_the_real_gate_stays_settled_only(tmp_path, capsys, monkeypatch):
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    log(storage, "att-1", 1)
    monkeypatch.undo()
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_TASK_ID", "unassociated-agent")
    # Refused before any log is read: either the node is live and settled-only, or the caller cannot be verified.
    message = inspect(capsys, run_id, "exec", expect=1)["message"]
    assert "fully settled" in message or "cannot be verified" in message


def live(capsys, run_id):
    return inspect(capsys, run_id, "exec")


def test_log_replacements_are_never_followed_or_blocked_on(tmp_path, capsys):
    import os
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    path = events_path(storage.root / "tasks", "att-1")
    outside = tmp_path / "outside.jsonl"
    outside.write_bytes(event_line("att-1", 7))
    path.symlink_to(outside)
    assert (live(capsys, run_id)["activity_state"], ) == ("unavailable",)
    path.unlink()
    os.mkfifo(path)
    result = live(capsys, run_id)
    assert (result["activity_state"], result["reason"], result["events"]) == ("unavailable", "log_not_regular_file", [])
    path.unlink()
    assert live(capsys, run_id)["activity_state"] == "not_started"


def test_read_uses_the_qualified_descriptor_when_the_path_is_swapped_after_open(tmp_path, capsys, monkeypatch):
    from harflow_mcp import events
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    log(storage, "att-1", 2)
    path = events_path(storage.root / "tasks", "att-1")
    outside = tmp_path / "outside.jsonl"
    outside.write_bytes(event_line("att-1", 9))
    original = events.read_page

    def swapping(*args, **kwargs):
        path.unlink()
        path.symlink_to(outside)
        return original(*args, **kwargs)
    monkeypatch.setattr(events, "read_page", swapping)
    result = live(capsys, run_id)
    assert result["activity_state"] == "available" and [event["seq"] for event in result["events"]] == [0, 1]


def test_read_failure_is_reported_as_unavailable(tmp_path, capsys, monkeypatch):
    from harflow_mcp import workflow_inspection

    class Broken:
        def seek(self, *args):
            return 100

        def read(self, *args):
            raise OSError("device error")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    monkeypatch.setattr(workflow_inspection, "_open_log", lambda path: ("available", None, Broken()))
    result = live(capsys, run_id)
    assert (result["activity_state"], result["reason"], result["events"]) == ("unavailable", "log_unreadable", [])


def test_request_documents_cannot_carry_the_permission(tmp_path):
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    run = storage.get_run(run_id)
    request = {"execution_id": "exec", "view": "activity"}
    with pytest.raises(ValueError, match="fully settled"):
        inspect_request(run, storage.root, request)
    with pytest.raises(ValueError, match="Unknown inspection request fields"):
        inspect_request(run, storage.root, {**request, "allow_live_activity": True})
    with pytest.raises(ValueError, match="Unknown inspection request fields"):
        inspect_request(run, storage.root, {**request, "allow_live_activity": True}, allow_live_activity=True)


# The settled MCP activity page of `test_mcp_never_gets_live_activity...`, without the run id, as the
# engine produced it before live activity existed.
MCP_SETTLED_ACTIVITY_SHA256 = "6742cc4ece6595f8f5edf98c5916646cf2f3b995bf809742d09abcd7f2d15d81"


async def test_mcp_never_gets_live_activity_and_settled_output_is_unchanged(tmp_path, monkeypatch):
    storage, run_id = build(tmp_path, [
        activation("done", "completed", {"task_id": "att-s", "status": "completed", "result": {"summary": "ok"}},
                   node_result={"status": "succeeded", "summary": "done"}),
        activation("live", "running", {"task_id": "att-1", "status": "running"})])
    log(storage, "att-s", 3)
    log(storage, "att-1", 1)
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=storage.root / "tasks"))
    page = await server.inspect_workflow_node(run_id, "done", view="activity")
    digest = hashlib.sha256(json.dumps({k: v for k, v in page.items() if k != "workflow_run_id"}, sort_keys=True).encode()).hexdigest()
    assert digest == MCP_SETTLED_ACTIVITY_SHA256, digest
    with pytest.raises(Exception, match="fully settled"):
        await server.inspect_workflow_node(run_id, "live", view="activity")


def test_task_events_serves_a_live_headless_attempt_to_a_direct_caller(tmp_path, capsys):
    from harflow_mcp import store
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    directory = storage.root / "tasks"
    store.write(directory, store.TaskRecord(
        task_id="att-1", backend="claude", session_id="s1", markers=["s1"], repo_path="/repo",
        started_at="2027-01-01T00:00:00+00:00", status="running", exit_code=None, output_drained=False))
    log(storage, "att-1", 2)
    assert ctl.main(["task-events", "att-1", "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert [event["seq"] for event in result["events"]] == [0, 1]


@pytest.mark.parametrize("error", [ValueError("rejected before reading"), OSError("state path refused"), KeyboardInterrupt()])
def test_the_log_stream_is_closed_when_the_page_read_raises_before_it_starts(tmp_path, monkeypatch, error):
    from harflow_mcp import events, workflow_inspection
    storage, run_id = build(tmp_path, [activation("exec", "running", {"task_id": "att-1", "status": "running"})])
    log(storage, "att-1", 2)
    opened = []
    real = workflow_inspection._open_log

    def tracking(path):
        found = real(path)
        opened.append(found[2])
        return found

    def refusing(*args, **kwargs):
        raise error
    monkeypatch.setattr(workflow_inspection, "_open_log", tracking)
    monkeypatch.setattr(events, "read_page", refusing)
    with pytest.raises(type(error)):
        inspect_request(storage.get_run(run_id), storage.root, {"execution_id": "exec", "view": "activity"}, allow_live_activity=True)
    assert len(opened) == 1 and opened[0].closed
