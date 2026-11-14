"""`cancel` refusal reason for workflow-owned native transports, through actual ctl dispatch."""
import contextlib
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import backends, ctl, identity, lineage, store, task_resources, task_supervision, tasks, workflows
from harflow_mcp import workflow_hooks
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from harflow_mcp.workflow_receipt_documents import encode

ROOT = Path(__file__).parents[1]
MESSAGE = "Internal native transports cannot be controlled independently; control the root workflow instead"
REASON = {"reason": "workflow_native_control"}


def generator():
    spec = importlib.util.spec_from_file_location("cancel_fixtures_native", ROOT / "scripts/generate-cancel-cascade-fixtures.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class World:
    def __init__(self, gen, layout, logs):
        self.gen, self.layout, self.logs = gen, layout, logs
        self.target = gen.TARGET

    def snapshot(self):
        return {str(p.relative_to(self.layout.root)): p.read_bytes() for p in sorted(self.layout.root.rglob("*")) if p.is_file() and "workflow" not in p.parts[-3:-1]}

    def run_document(self):
        storage = workflows.WorkflowStore(self.layout.root)
        return storage, storage.get_run(self.run_id)

    def native_run(self, task_id=None, role="native_control"):
        gen = self.gen
        (self.layout.root / "repo").mkdir(exist_ok=True)
        storage = workflows.WorkflowStore(self.layout.root)
        definition = {"name": "fixture", "orchestrator": {"backend": "codex"}, "nodes": [
            {"id": "start", "type": "start", "prompt": "fixture"},
            {"id": "work", "type": "agent", "instructions": "fixture", "agent": {"backend": "codex"}},
            {"id": "end", "type": "end"}],
            "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]}
        run = storage.create_run(workflows.validate_definition(definition), "fixture", self.layout.root / "repo")
        storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running", activations=[
            {"id": "transport", "node_id": "work", "role": role, "status": "running", "created_at": 0.0,
             "tasks": [{"task_id": task_id or gen.TARGET, "status": "running"}]}]), "fixture")
        self.run_id = run["workflow_run_id"]
        return self.run_id

    def run(self, *argv, capsys):
        code = ctl.main(list(argv))
        out = capsys.readouterr()
        return code, out.out, out.err


@pytest.fixture
def world(tmp_path, monkeypatch):
    gen = generator()
    layout = resolve_layout(root=tmp_path / "state")
    with contextlib.ExitStack() as stack:
        stack.enter_context(state_session(layout))
        logs = layout.tasks_directory
        logs.mkdir(parents=True)
        monkeypatch.setenv("HARFLOW_STATE_ROOT", str(layout.root))
        monkeypatch.delenv(lineage.ENV_TASK_ID, raising=False)
        monkeypatch.setattr(ctl, "default_log_dir", lambda: logs)
        monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: None)
        monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None))
        monkeypatch.setattr(identity, "own_identity", lambda: gen.OWNER)
        monkeypatch.setattr(identity, "identity_check", lambda value: "dead")
        monkeypatch.setattr(identity, "check_detail", lambda value: ("dead", "start_time_differs"))
        monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
        yield World(gen, layout, logs)


def effects(monkeypatch):
    """Spies for every control effect a refused cancel must not reach."""
    seen = []
    monkeypatch.setattr(workflows.WorkflowStore, "control", lambda self, *a, **k: seen.append("pause"))
    monkeypatch.setattr(tasks.TaskRegistry, "_cancel_cascade", lambda *a, **k: seen.append("cascade"))
    monkeypatch.setattr(task_resources, "cancel_launch", lambda *a, **k: seen.append("cancel_launch"))
    monkeypatch.setattr(task_supervision, "request_cancel", lambda *a, **k: seen.append("request"), raising=False)
    return seen


def error(out):
    return json.loads(out)["error"]


def test_pending_launch_native_control_refusal_carries_reason(world, monkeypatch, capsys):
    world.gen.receipt(world.logs, world.target)
    world.native_run()
    seen = effects(monkeypatch)
    before = world.snapshot()
    code, out, err = world.run("cancel", world.target, "--json", capsys=capsys)
    assert code == 1
    assert error(out) == {"code": "invalid_params", "data": REASON, "message": MESSAGE}
    assert "result" not in json.loads(out)
    assert err == f"harflow-ctl: error: {MESSAGE}\n"
    assert seen == []
    assert world.snapshot() == before  # receipt bytes unchanged, no withdrawal or cancel-request file


def test_recorded_task_native_control_refusal_carries_reason(world, monkeypatch, capsys):
    world.gen.record(world.logs, world.target, status="running", exit_code=None, output_drained=False, finished_at=None)
    world.native_run()
    seen = effects(monkeypatch)
    before = world.snapshot()
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    assert code == 1
    assert error(out) == {"code": "invalid_params", "data": REASON, "message": MESSAGE}
    assert seen == []
    assert world.snapshot() == before


def test_native_control_text_mode_is_unchanged(world, capsys):
    world.gen.receipt(world.logs, world.target)
    world.native_run()
    code, out, err = world.run("cancel", world.target, capsys=capsys)
    assert (code, out, err) == (1, "", f"harflow-ctl: error: {MESSAGE}\n")


def test_ancestor_association_with_native_control_role_also_refused(world, capsys):
    world.gen.record(world.logs, world.gen.CHILD, spawned_by=world.target, status="running", exit_code=None, output_drained=False, finished_at=None)
    world.native_run()
    code, out, _ = world.run("cancel", world.gen.CHILD, "--json", capsys=capsys)
    assert code == 1 and error(out)["data"] == REASON


def test_non_native_workflow_owner_is_not_refused_with_the_reason(world, monkeypatch, capsys):
    world.gen.receipt(world.logs, world.target)
    world.native_run(role="node")
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    doc = json.loads(out)
    assert "error" not in doc or "data" not in doc["error"]


def test_native_refusal_exception_is_a_workflow_error_with_unchanged_text():
    exc = workflows.NativeControlRefused(MESSAGE)
    assert isinstance(exc, workflows.WorkflowError) and isinstance(exc, ValueError)
    assert exc.reason == "workflow_native_control" and str(exc) == MESSAGE


def test_refuse_native_control_raises_only_on_a_positive_match(world):
    world.native_run(role="node")
    workflow_hooks.refuse_native_control(world.logs, world.target)  # owned by a node: not refused
    workflow_hooks.refuse_native_control(world.logs, "nobody")
    world.native_run(task_id="transport")
    with pytest.raises(workflows.NativeControlRefused):
        workflow_hooks.refuse_native_control(world.logs, "transport")


def test_unestablished_ownership_carries_no_reason(world, capsys):
    # A receipt naming a run that does not contain the task: ownership cannot be established.
    world.gen.receipt(world.logs, world.target)
    other = world.native_run(task_id="someone-else")
    owners = world.layout.root / "workflow-owners"
    owners.mkdir(exist_ok=True)
    task_supervision._write(owners / f"{world.target}.json", encode(dict(
        workflow_run_id=other, root_workflow_run_id=other, workflow_execution_id="transport"), world.target))
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    assert code == 1
    refusal = error(out)
    assert refusal["code"] == "invalid_params" and "data" not in refusal


def test_unreadable_run_document_keeps_its_existing_code_without_reason(world, capsys):
    world.gen.receipt(world.logs, world.target)
    run_id = world.native_run()
    for path in world.layout.root.rglob(f"{run_id}*"):
        if path.is_file():
            path.write_bytes(b"{not json")
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    refusal = error(out)
    assert code == 1 and refusal["code"] == "workflow_run_incompatible"
    assert (refusal.get("data") or {}).get("reason") != "workflow_native_control"


def test_managed_caller_refusal_has_no_reason_and_precedes_target_lookup(world, monkeypatch, capsys):
    world.gen.receipt(world.logs, world.target)
    world.native_run()
    other = world.native_run(task_id="manager", role="node")
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: SimpleNamespace(record=SimpleNamespace(task_id="manager")))
    monkeypatch.setattr("harflow_mcp.caller_policy.require_mutation", lambda *a, **k: None)
    looked_up = []
    monkeypatch.setattr(ctl, "_resolve_task_arg", lambda *a, **k: looked_up.append(a) or pytest.fail("target lookup before authority"))
    monkeypatch.setattr(workflow_hooks, "refuse_native_control", lambda *a: looked_up.append("native") or pytest.fail("native check before authority"))
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    assert code == 1
    refusal = error(out)
    assert refusal["code"] == "invalid_params" and "data" not in refusal and "supervisor" in refusal["message"]
    assert looked_up == []


def test_undecidable_authority_refusal_has_no_reason(world, monkeypatch, capsys):
    world.gen.receipt(world.logs, world.target)
    world.native_run()
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "ps denied"))
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    refusal = error(out)
    assert code == 1 and refusal["code"] == "invalid_params" and "data" not in refusal and "ps denied" in refusal["message"]


def test_unverifiable_task_id_environment_refusal_has_no_reason(world, monkeypatch, capsys):
    world.gen.receipt(world.logs, world.target)
    world.native_run()
    monkeypatch.setenv(lineage.ENV_TASK_ID, "claimed-task")
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    refusal = error(out)
    assert code == 1 and "data" not in refusal and "reason" not in refusal


def test_unknown_task_keeps_its_own_data_and_never_the_native_reason(world, capsys):
    code, out, _ = world.run("cancel", "missing-task", "--json", capsys=capsys)
    refusal = error(out)
    assert code == 1 and refusal["code"] == "unknown_task"
    assert refusal["data"] == {"code": "unknown_task", "input": "missing-task", "reason": "no_prefix_match"}


@pytest.mark.parametrize("exc", [ValueError("unrelated refusal"), backends.UnsupportedCapability("unsupported here")])
def test_other_refusals_keep_their_shape_on_both_paths(world, monkeypatch, capsys, exc):
    def boom(*a, **k):
        raise exc
    world.gen.receipt(world.logs, world.target)
    monkeypatch.setattr(tasks.TaskRegistry, "cancel_pending", boom)
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    assert code == 1 and error(out) == {"code": "invalid_params", "message": str(exc)}
    world.gen.record(world.logs, "recorded-one")
    monkeypatch.setattr(tasks.TaskRegistry, "cancel_cascade", boom)
    code, out, _ = world.run("cancel", "recorded-one", "--json", capsys=capsys)
    assert code == 1 and error(out) == {"code": "invalid_params", "message": str(exc)}


def test_launch_that_spawns_between_read_and_cancel_reports_reason_or_unknown_task(world, monkeypatch, capsys):
    world.gen.receipt(world.logs, world.target)
    world.native_run()
    real = store.read
    calls = []
    def stale_first(log_dir, task_id, *a, **k):
        calls.append(task_id)
        return None if len(calls) == 1 else real(log_dir, task_id, *a, **k)
    monkeypatch.setattr(ctl.store, "read", stale_first)
    code, out, _ = world.run("cancel", world.target, "--json", capsys=capsys)
    refusal = error(out)
    assert code == 1 and (refusal.get("data") == REASON or refusal["code"] == "unknown_task")


async def test_registry_refusal_text_and_type_hierarchy_are_unchanged_for_mcp_callers(world):
    world.gen.receipt(world.logs, world.target)
    world.native_run()
    registry = tasks.TaskRegistry(log_dir=world.logs, open_monitor=False)
    with pytest.raises(workflows.WorkflowError, match="control the root workflow instead") as caught:
        await registry.cancel_pending(world.target)
    assert str(caught.value) == MESSAGE
    assert type(caught.value) is workflows.NativeControlRefused
