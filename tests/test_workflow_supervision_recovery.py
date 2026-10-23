from pathlib import Path
from types import SimpleNamespace

from harflow_mcp import store, supervision_notifications, workflow_notifications


def _record(task_id: str):
    return store.TaskRecord(
        task_id=task_id,
        backend="claude",
        session_id="session-" + task_id,
        repo_path="/repo",
        started_at="2026-10-23T00:00:00+00:00",
        status="running",
        owner={"pid": 1, "start_time": None, "markers": []},
        terminal_summary="lost",
    )


class _Storage:
    def __init__(self, runs):
        self.runs = runs
        self.owners = Path("/owners")
        self.reads = 0

    def get_run(self, run_id, **kwargs):
        self.reads += 1
        return self.runs.get(run_id)


def _fixture(count=65):
    tasks = [f"task-{index:03d}" for index in range(count)]
    activation = {
        "id": "execution-root",
        "node_id": "node-root",
        "tasks": [{"task_id": task_id} for task_id in tasks],
    }
    root = {"workflow_run_id": "root", "activations": [activation]}
    return root, tasks


def test_workflow_nodes_fairly_reaches_all_qualified_siblings_with_bounded_reads(monkeypatch, tmp_path):
    root, task_ids = _fixture()
    storage = _Storage({})
    records = {task_id: _record(task_id) for task_id in task_ids}
    reads_per_call = []

    monkeypatch.setattr(workflow_notifications, "_storage", lambda _log_dir: storage)
    monkeypatch.setattr(
        "harflow_mcp.bounded_io.read_receipt",
        lambda _path: {
            "workflow_run_id": "root",
            "workflow_node_id": "node-root",
            "workflow_execution_id": "execution-root",
        },
    )
    monkeypatch.setattr(
        supervision_notifications.store,
        "read",
        lambda _log_dir, task_id, **kwargs: records.get(task_id),
    )
    monkeypatch.setattr(
        supervision_notifications,
        "loss",
        lambda record, _log_dir: ("generation", {"capture_state": "lost_with_owner"}),
    )
    supervision_notifications._SCAN_CURSORS.clear()

    seen = set()
    for _ in range(3):
        before = len([key for key in records if key])
        nodes, truncated = supervision_notifications._workflow_nodes(tmp_path, root)
        reads_per_call.append(len(nodes))
        seen.update(node[1]["task_id"] for node in nodes)
        assert len(nodes) <= supervision_notifications.NODE_LIMIT
        assert isinstance(truncated, bool)
    assert seen == set(task_ids)
    assert reads_per_call == [64, 1, 64]


def test_workflow_nodes_keeps_verified_root_task_when_child_run_is_missing(monkeypatch, tmp_path):
    root = {
        "workflow_run_id": "root",
        "activations": [{
            "id": "execution-root",
            "node_id": "node-root",
            "tasks": [{"task_id": "root-task"}],
            "invocation": {"child_workflow_run_id": "missing-child"},
        }],
    }
    storage = _Storage({})
    monkeypatch.setattr(workflow_notifications, "_storage", lambda _log_dir: storage)
    monkeypatch.setattr(
        "harflow_mcp.bounded_io.read_receipt",
        lambda _path: {
            "workflow_run_id": "root",
            "workflow_node_id": "node-root",
            "workflow_execution_id": "execution-root",
        },
    )
    monkeypatch.setattr(supervision_notifications.store, "read", lambda *_args, **_kwargs: _record("root-task"))
    monkeypatch.setattr(
        supervision_notifications,
        "loss",
        lambda record, _log_dir: ("generation", {"capture_state": "lost_with_owner"}),
    )
    supervision_notifications._SCAN_CURSORS.clear()

    nodes, truncated = supervision_notifications._workflow_nodes(tmp_path, root)

    assert [node[1]["task_id"] for node in nodes] == ["root-task"]
    assert truncated is True
