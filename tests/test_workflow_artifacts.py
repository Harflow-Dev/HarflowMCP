"""Focused contracts for workflow-owned artifact references (#82)."""

import json
from pathlib import Path

import pytest

from polybridge import artifacts, store, workflows
from polybridge.bounded_io import ReadLimit


def _item(task_id: str, *, artifact_id: str = "00000000-0000-4000-8000-000000000001", workflow=None, note="note"):
    value = {
        "artifact_id": artifact_id,
        "task_id": task_id,
        "path": "/repo/report.txt",
        "kind": "report",
        "note": note,
        "registered_at": "2026-10-01T00:00:00+00:00",
        "provenance": {"method": "verified_process_lineage", "task_id": task_id},
        "registered_observation": {"device": 1, "inode": 2, "size": 3, "mtime_ns": 4},
    }
    if workflow is not None:
        value["workflow"] = workflow
    return value


def _binding(task_id="worker", *, role="node", run_id="run", execution_id="exec"):
    return {
        "root_workflow_run_id": run_id,
        "workflow_run_id": run_id,
        "node_id": "work",
        "execution_id": execution_id,
        "role": role,
        "task_id": task_id,
        "attribution": "native_transport" if role == "native_control" else "attempt",
        "provenance": "workflow_receipt",
        **({"hosted_native_attempt_ids": ["child-exec"]} if role == "native_control" else {}),
    }


def test_manifest_workflow_binding_is_optional_but_strict(tmp_path):
    logs = tmp_path / "tasks"
    logs.mkdir()
    task_id = "worker"
    path = logs / f"{task_id}.artifacts.json"
    path.write_text(json.dumps({"v": 1, "task_id": task_id, "items": [_item(task_id, workflow=_binding(task_id))]}))
    assert artifacts._load(logs, task_id)[0][0]["workflow"]["execution_id"] == "exec"

    malformed = _item(task_id, workflow=_binding(task_id) | {"task_id": "other"})
    path.write_text(json.dumps({"v": 1, "task_id": task_id, "items": [malformed]}))
    with pytest.raises(ValueError, match="manifest workflow binding"):
        artifacts._load(logs, task_id)


def test_tree_members_uses_catalog_without_loading_run_records(tmp_path, monkeypatch):
    storage = workflows.WorkflowStore(tmp_path)
    monkeypatch.setattr(storage, "list_run_page", lambda *a, **k: {"bootstrap_pending": False, "history_incomplete": False})
    monkeypatch.setattr(storage, "get_run", lambda *a, **k: pytest.fail("tree discovery decoded a run record"))
    class DB:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, *args):
            class Rows:
                def fetchone(self): return None
                def fetchall(self): return [("root", 1.0, None, "root")]
            return Rows()
    class Catalog:
        def __init__(self, *args): pass
        def connect(self): return DB()
        def _discover(self, db): pass
        def state(self, db): return {"status": "ready"}
    monkeypatch.setattr("polybridge.catalog.Catalog", Catalog)
    result = storage.tree_members("root")
    assert result["state"] == "ready"
    assert result["members"][0]["id"] == "root"


def test_workflow_page_cold_catalog_is_retryable_without_partial_reads(tmp_path, monkeypatch):
    class ColdStore:
        def __init__(self, root):
            self.root = Path(root)

        def tree_members(self, run_id, limit):
            return {"state": "preparing", "members": []}

        def prepare_run_catalog(self):
            return {"state": "preparing"}

    monkeypatch.setattr(workflows, "WorkflowStore", ColdStore)
    monkeypatch.setattr(artifacts, "_load", lambda *a, **k: pytest.fail("cold catalog must not read manifests"))
    page = artifacts.workflow_page(tmp_path / "tasks", "root", limit=1)
    assert page["state"] == "unavailable"
    assert page["reason"] == "catalog_preparing"


def test_workflow_page_retries_after_catalog_preparation(tmp_path, monkeypatch):
    class EventuallyReady:
        prepared = False

        def __init__(self, root):
            self.root = Path(root)

        def tree_members(self, run_id, limit):
            if not self.prepared:
                return {"state": "preparing", "members": []}
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}

        def prepare_run_catalog(self):
            type(self).prepared = True

        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": []}

    monkeypatch.setattr(workflows, "WorkflowStore", EventuallyReady)
    first = artifacts.workflow_page(tmp_path / "tasks", "root")
    assert first["reason"] == "catalog_preparing"
    second = artifacts.workflow_page(tmp_path / "tasks", "root")
    assert second["state"] == "available"


def test_workflow_page_rejects_cursor_into_empty_activation(tmp_path, monkeypatch):
    class FakeStore:
        def __init__(self, root):
            self.root = Path(root)

        def tree_members(self, run_id, limit):
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}

        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": [{"id": "empty", "tasks": []}]}

    monkeypatch.setattr(workflows, "WorkflowStore", FakeStore)
    cursor = artifacts._page_cursor("root", ("root", 0, 0), "owner", None)
    with pytest.raises(ValueError, match="Invalid or stale"):
        artifacts.workflow_page(tmp_path / "tasks", "root", cursor=cursor)


def test_workflow_page_converts_owner_read_budget_to_unavailable_owner(tmp_path, monkeypatch):
    class FakeStore:
        def __init__(self, root):
            self.root = Path(root)

        def tree_members(self, run_id, limit):
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}

        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": [{"id": "exec", "node_id": "work", "role": "node", "tasks": [{"task_id": "owner"}]}]}

    monkeypatch.setattr(workflows, "WorkflowStore", FakeStore)
    monkeypatch.setattr(artifacts, "_load", lambda *a, **k: ([_item("owner", run_id="root", workflow=_binding("owner", run_id="root"))], b"manifest"))
    monkeypatch.setattr(artifacts, "_read_owner", lambda *a, **k: (_ for _ in ()).throw(ReadLimit("budget")))
    page = artifacts.workflow_page(tmp_path / "tasks", "root")
    assert page["owners"] == [{"task_id": "owner", "workflow_run_id": "root", "execution_id": "exec", "state": "unavailable"}]


def test_workflow_page_keeps_corrupt_existing_owner_as_unavailable(tmp_path, monkeypatch):
    class FakeStore:
        def __init__(self, root):
            self.root = Path(root)

        def tree_members(self, run_id, limit):
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}

        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": [{"id": "exec", "node_id": "work", "role": "node", "tasks": [{"task_id": "owner"}]}]}

    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    (log_dir / "owner.meta.json").write_text("corrupt")
    monkeypatch.setattr(workflows, "WorkflowStore", FakeStore)
    monkeypatch.setattr(artifacts, "_load", lambda *a, **k: ([_item("owner", run_id="root", workflow=_binding("owner", run_id="root"))], b"manifest"))
    monkeypatch.setattr(artifacts, "_read_owner", lambda *a, **k: None)
    page = artifacts.workflow_page(log_dir, "root")
    assert page["owners"] == [{"task_id": "owner", "workflow_run_id": "root", "execution_id": "exec", "state": "unavailable"}]
    assert page["items"] == []


def test_workflow_page_budget_exhaustion_advances_owner_cursor(tmp_path, monkeypatch):
    owner = "owner"

    class FakeStore:
        def __init__(self, root):
            self.root = Path(root)

        def tree_members(self, run_id, limit):
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}

        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": [{"id": "exec", "node_id": "work", "role": "node", "tasks": [{"task_id": owner}, {"task_id": "second"}, {"task_id": "third"}]}]}

    monkeypatch.setattr(workflows, "WorkflowStore", FakeStore)
    calls = 0
    def load(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ReadLimit("budget")
        return ([_item("second", artifact_id="00000000-0000-4000-8000-000000000002", workflow=_binding("second", run_id="root", execution_id="exec"))], b"manifest")
    monkeypatch.setattr(artifacts, "_load", load)
    monkeypatch.setattr(artifacts, "_read_owner", lambda *a, **k: store.TaskRecord("second", "codex", None, "/repo", "2026-01-01T00:00:00+00:00"))
    monkeypatch.setattr(artifacts, "_availability", lambda *a, **k: "available")
    page = artifacts.workflow_page(tmp_path / "tasks", "root", limit=1)
    assert page["has_more"]
    assert page["next_cursor"]
    assert page["items"][0]["task_id"] == "second"


def test_selected_artifact_refs_are_frozen_and_notes_render_verbatim(monkeypatch, tmp_path):
    from polybridge import workflow_artifact_refs as refs

    selected = _item("owner", note="  preserve\nexactly  ") | {
        "availability": {"state": "available"},
        "registered_at": "2026-10-01T00:00:00+00:00",
    }
    execution = {"id": "exec", "node_id": "work", "role": "node", "status": "completed", "tasks": [{"task_id": "owner"}]}
    run = {"workflow_run_id": "run", "activations": [execution]}
    monkeypatch.setattr(refs, "_find_execution", lambda *a, **k: (run, execution))
    monkeypatch.setattr(refs, "execution_items", lambda *a, **k: [{
        "execution_id": "exec", "artifact_id": selected["artifact_id"], "owner_task_id": "owner",
        "workflow_run_id": "run", "node_id": "work", "kind": "report", "path": selected["path"],
        "note": selected["note"], "registered_at": selected["registered_at"], "registered_observation": selected["registered_observation"],
        "availability_at_assignment": "available", "attribution": "attempt",
    }])
    frozen = refs.resolve_refs(run, [{"execution_id": "exec", "artifact_id": selected["artifact_id"]}], tmp_path)
    assert frozen[0]["note"] == selected["note"]
    rendered = refs.render_section(frozen)
    payload = json.loads(rendered.split("\n", 3)[3])
    assert payload[0]["note"] == selected["note"]


@pytest.mark.parametrize("bad", [
    [{"execution_id": "exec", "artifact_id": "missing"}, {"execution_id": "exec", "artifact_id": "missing"}],
    [{"execution_id": "exec", "artifact_id": "missing", "extra": "field"}],
])
def test_artifact_ref_selection_rejects_duplicates_and_extra_fields(tmp_path, bad):
    from polybridge import workflow_artifact_refs as refs
    with pytest.raises(workflows.WorkflowError, match="artifact_refs"):
        refs.resolve_refs({"workflow_run_id": "run"}, bad, tmp_path)


def test_worker_prompt_keeps_selected_refs_with_optimized_delivery(tmp_path):
    from polybridge.workflow_delegation import worker_prompt

    note = "literal note\nwith leading spaces"
    token = {
        "assignment_prompt": "Do the assigned work",
        "assigned_task_ids": [],
        "authorized_artifact_refs": [{"execution_id": "exec", "artifact_id": "artifact", "node_id": "work", "kind": "report", "path": "/repo/report", "note": note, "availability_at_assignment": "available", "attribution": "attempt"}],
    }
    run = {"workflow_run_id": "run", "prompt": "original", "definition": {"nodes": [{"id": "work", "type": "agent", "role": "task", "instructions": "instructions"}], "connections": []}, "activations": [], "tasks": []}
    node = run["definition"]["nodes"][0]
    prompt = worker_prompt(run, node, token, root=tmp_path)
    assert prompt.index("Selected artifact references:") < prompt.index("Input results:")
    selected = json.loads(prompt.split("Selected artifact references:\n", 1)[1].split("\nInput results:", 1)[0].split("\n", 1)[1])
    assert selected[0]["note"] == note


@pytest.mark.parametrize("role", ["node", "orchestrator", "native_control"])
def test_authority_binds_live_workflow_roles_and_native_transport(tmp_path, monkeypatch, role):
    record = store.TaskRecord("worker", "codex", None, "/repo", "2026-10-01T00:00:00+00:00", status="running")
    receipt = {"workflow_run_id": "run", "root_workflow_run_id": "run", "workflow_execution_id": "exec", "workflow_role": role}
    activation = {"id": "exec", "node_id": "work", "role": role, "status": "running", "tasks": [{"task_id": "worker", "status": "running", **({"native_owner_execution_ids": ["child"]} if role == "native_control" else {})}]}
    run = {"workflow_run_id": "run", "kind": "workflow", "activations": [activation]}
    monkeypatch.setattr(artifacts, "_workflow_owner", lambda *a, **k: {"receipt": receipt})
    monkeypatch.setattr(artifacts, "_read_owner", lambda *a, **k: record)
    monkeypatch.setattr(workflows.WorkflowStore, "get_run", lambda *a, **k: run)
    binding = artifacts._authority(tmp_path / "tasks", record, registering=True)
    assert binding["role"] == role
    assert binding["attribution"] == ("native_transport" if role == "native_control" else "attempt")
    if role == "native_control":
        assert binding["hosted_native_attempt_ids"] == ["child"]


@pytest.mark.parametrize("change", [
    {"workflow_run_id": "other", "root_workflow_run_id": "other"},
    {"workflow_execution_id": "wrong"},
    {"workflow_role": "builder"},
])
def test_authority_refuses_receipt_or_run_mismatch(tmp_path, monkeypatch, change):
    record = store.TaskRecord("worker", "codex", None, "/repo", "2026-10-01T00:00:00+00:00", status="running")
    receipt = {"workflow_run_id": "run", "root_workflow_run_id": "run", "workflow_execution_id": "exec", "workflow_role": "node"} | change
    run = {"workflow_run_id": "run", "kind": "workflow", "activations": [{"id": "exec", "node_id": "work", "role": "node", "status": "completed", "tasks": [{"task_id": "worker", "status": "completed"}]}]}
    monkeypatch.setattr(artifacts, "_workflow_owner", lambda *a, **k: {"receipt": receipt})
    monkeypatch.setattr(workflows.WorkflowStore, "get_run", lambda *a, **k: run)
    with pytest.raises(ValueError, match="matches its run|role|no longer running"):
        artifacts._authority(tmp_path / "tasks", record, registering=True)


def test_authority_accepts_managed_ancestor_in_same_tree_and_refuses_other_tree(tmp_path, monkeypatch):
    child = store.TaskRecord("child", "codex", None, "/repo", "2026-10-01T00:00:00+00:00", status="running", spawned_by="parent")
    parent = store.TaskRecord("parent", "codex", None, "/repo", "2026-10-01T00:00:00+00:00", status="running")
    child_receipt = {"workflow_run_id": "run", "root_workflow_run_id": "run", "workflow_execution_id": "child-exec", "workflow_role": "node"}
    parent_receipt = {"workflow_run_id": "run", "root_workflow_run_id": "run", "workflow_execution_id": "parent-exec", "workflow_role": "orchestrator"}
    run = {"workflow_run_id": "run", "kind": "workflow", "activations": [{"id": "child-exec", "node_id": "work", "role": "node", "status": "running", "tasks": [{"task_id": "child", "status": "running"}]}, {"id": "parent-exec", "node_id": "root", "role": "orchestrator", "status": "running", "tasks": [{"task_id": "parent", "status": "running"}]}]}
    monkeypatch.setattr(artifacts, "_workflow_owner", lambda directory, task_id, strict: {"receipt": child_receipt if task_id == "child" else parent_receipt})
    monkeypatch.setattr(artifacts, "_read_owner", lambda directory, task_id: parent if task_id == "parent" else child)
    monkeypatch.setattr(workflows.WorkflowStore, "get_run", lambda *a, **k: run)
    assert artifacts._authority(tmp_path / "tasks", child)["root_workflow_run_id"] == "run"
    monkeypatch.setattr(artifacts, "_workflow_owner", lambda directory, task_id, strict: {"receipt": child_receipt if task_id == "child" else parent_receipt | {"root_workflow_run_id": "other", "workflow_run_id": "other"}})
    with pytest.raises(ValueError, match="another workflow tree|matches its run"):
        artifacts._authority(tmp_path / "tasks", child)


@pytest.mark.parametrize("managed", [None, ({"role": "node"}, {"workflow_run_id": "run"}), ({"role": "builder"}, {"workflow_run_id": "run"})])
def test_run_level_artifact_read_authority_is_role_scoped(tmp_path, monkeypatch, managed):
    from polybridge import workflow_node_reads
    observed = {}
    monkeypatch.setattr(artifacts, "workflow_page", lambda *a, **k: observed.update(kwargs=k) or {"state": "available"})
    if managed is None:
        assert workflow_node_reads.workflow_artifacts_page(None, True, tmp_path / "tasks", "run")["state"] == "available"
    else:
        with pytest.raises(ValueError, match="human|orchestrator"):
            workflow_node_reads.workflow_artifacts_page(managed, False, tmp_path / "tasks", "run")


def test_inherited_artifact_refs_are_selectable_only_when_explicitly_forwarded(tmp_path, monkeypatch):
    from polybridge import workflow_artifact_refs as refs
    inherited = {"execution_id": "parent-exec", "artifact_id": "artifact", "owner_task_id": "owner", "workflow_run_id": "parent", "node_id": "work", "kind": "report", "path": "/repo/r", "note": "keep", "registered_at": "now", "registered_observation": {}, "availability_at_assignment": "available", "attribution": "attempt"}
    run = {"workflow_run_id": "child", "invocation_artifact_refs": [inherited], "activations": []}
    monkeypatch.setattr(refs, "_find_execution", lambda *a, **k: None)
    monkeypatch.setattr(artifacts, "owner_references", lambda *a, **k: [{"artifact_id": "artifact", "availability": {"state": "available"}, "workflow": {"task_id": "owner", "workflow_run_id": "parent"}}])
    frozen = refs.resolve_refs(run, [{"execution_id": "parent-exec", "artifact_id": "artifact"}], tmp_path)
    assert frozen[0]["inherited"] is True
    assert "Selected artifact references:" not in refs.render_section([])


def test_workflow_retry_registration_keeps_distinct_artifact_ids(tmp_path, monkeypatch):
    logs = tmp_path / "bridge" / "tasks"; logs.mkdir(parents=True)
    repo = tmp_path / "repo"; repo.mkdir()
    record = store.TaskRecord("owner", "codex", None, str(repo), "2026-01-01T00:00:00+00:00", pid=123, pgid=456, start_time="start")
    store.write(logs, record)
    from polybridge import identity, lineage
    monkeypatch.setattr(lineage, "_default_process_table", lambda: {__import__('os').getpid(): 123, 123: 1, 1: 0})
    monkeypatch.setattr(identity, "identity_check", lambda _: "alive")
    target = repo / "report"
    target.write_text("first")
    binding = _binding(record.task_id)
    monkeypatch.setattr(artifacts, "_authority", lambda *a, **k: binding)
    first = artifacts.register(logs, str(target), "report", "retry output")
    target.write_text("second")
    second = artifacts.register(logs, str(target), "report", "retry output")
    assert first["artifact_id"] != second["artifact_id"]
    assert [item["workflow"]["execution_id"] for item in artifacts.get(logs, record.task_id)["items"]] == ["exec", "exec"]


def test_workflow_page_reports_deleted_changed_and_record_removed_owners(tmp_path, monkeypatch):
    class FakeStore:
        def __init__(self, root): self.root = Path(root)
        def tree_members(self, run_id, limit):
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}
        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": [{"id": "e", "node_id": "work", "role": "node", "tasks": [{"task_id": "deleted"}, {"task_id": "changed"}, {"task_id": "missing"}]}]}
    monkeypatch.setattr(workflows, "WorkflowStore", FakeStore)
    manifests = {"deleted": [_item("deleted", artifact_id="00000000-0000-4000-8000-000000000010", workflow=_binding("deleted", run_id="root", execution_id="e"))], "changed": [_item("changed", artifact_id="00000000-0000-4000-8000-000000000011", workflow=_binding("changed", run_id="root", execution_id="e"))]}
    monkeypatch.setattr(artifacts, "_load", lambda log, owner: (manifests[owner], b"manifest") if owner in manifests else ([], b""))
    monkeypatch.setattr(artifacts, "_read_owner", lambda log, owner: store.TaskRecord(owner, "codex", None, "/repo", "now"))
    states = iter(["deleted", "changed"])
    monkeypatch.setattr(artifacts, "_availability", lambda *a, **k: next(states))
    page = artifacts.workflow_page(tmp_path / "tasks", "root")
    assert [item["availability"]["state"] for item in page["items"]] == ["deleted", "changed"]
    assert any(owner["state"] == "record_removed" for owner in page["owners"])


def test_workflow_artifact_cursor_rejects_bad_position(tmp_path, monkeypatch):
    class FakeStore:
        def __init__(self, root): self.root = Path(root)
        def tree_members(self, run_id, limit):
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}
        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": [{"id": "e", "role": "node", "tasks": []}]}
    monkeypatch.setattr(workflows, "WorkflowStore", FakeStore)
    cursor = artifacts._page_cursor("root", ("root", 99, 0), "owner", None)
    with pytest.raises(ValueError, match="Invalid or stale"):
        artifacts.workflow_page(tmp_path / "tasks", "root", cursor=cursor)


def test_workflow_page_marks_catalog_stale_when_linked_child_is_missing(tmp_path, monkeypatch):
    class FakeStore:
        def __init__(self, root): self.root = Path(root); self.runs = self.root / "workflow-runs"
        def tree_members(self, run_id, limit):
            return {"state": "ready", "members": [{"id": "root", "created_at": 1, "parent": None, "root": "root"}]}
        def get_run(self, run_id, **kwargs):
            return {"workflow_run_id": "root", "activations": [{"id": "e", "role": "node", "tasks": [], "invocation": {"child_workflow_run_id": "missing"}}]}
    monkeypatch.setattr(workflows, "WorkflowStore", FakeStore)
    (tmp_path / "workflow-runs").mkdir()
    (tmp_path / "workflow-runs" / "missing.json").write_text("{}")
    page = artifacts.workflow_page(tmp_path / "tasks", "root")
    assert page["state"] == "unavailable"
    assert page["reason"] == "catalog_stale"
