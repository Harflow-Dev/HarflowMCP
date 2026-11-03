"""`harflow-ctl task-list-page --predecessor-task-id`: direct-caller successor discovery."""
from __future__ import annotations

import json
import time

import pytest

from harflow_mcp import ctl, lineage, state_ownership, store, takeover
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CAPABILITIES

ROOT = "11111111-aaaa-4aaa-8aaa-111111111111"
OTHER = "22222222-bbbb-4bbb-8bbb-222222222222"


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    for key in ("HARFLOW_STATE_ROOT", "HARFLOW_TASK_ID", "HARFLOW_DEPTH"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield layout.tasks_directory


def write(directory, task_id, **overrides):
    base = {"task_id": task_id, "backend": "claude", "session_id": "s-" + task_id, "markers": [],
            "repo_path": "/repo", "started_at": "2027-01-01T00:00:00+00:00", "status": "completed",
            "exit_code": 0, "finished_at": "2027-01-01T00:00:10+00:00", "output_drained": True}
    store.write(directory, store.TaskRecord(**(base | overrides)))


def run(capsys, *args, expect=0):
    code = ctl.main(["task-list-page", *args, "--json"])
    assert code == expect
    document = json.loads(capsys.readouterr().out.splitlines()[-1])
    return document["result"] if expect == 0 else document["error"]


def warm(capsys):
    for _ in range(10):
        if not run(capsys)["bootstrap_pending"]:
            return
    raise AssertionError("catalog never became ready")


def seed(directory):
    write(directory, ROOT)
    write(directory, "resume-1", parent_task_id=ROOT, started_at="2027-01-01T00:01:00+00:00")
    write(directory, "continue-1", continuation_of_task_id=ROOT, chain_root_task_id=ROOT, started_at="2027-01-01T00:02:00+00:00")
    write(directory, OTHER)
    write(directory, "other-child", parent_task_id=OTHER, status="running", exit_code=None, finished_at=None, output_drained=False)


def test_capability_is_declared():
    assert "task-successors.v1" in CAPABILITIES


def test_lists_resumes_and_continuations_newest_first_with_scope(state, capsys):
    seed(state)
    warm(capsys)
    result = run(capsys, "--predecessor-task-id", ROOT)
    assert [item["task_id"] for item in result["items"]] == ["continue-1", "resume-1"]
    assert result["predecessor_task_id"] == ROOT and result["successor_scope"] == "recorded_tasks"
    assert "resolved_from_predecessor_task_id" not in result
    assert all(not {"predecessor_task_id", "successor_scope"} & set(item) for item in result["items"])


def test_prefix_is_resolved_and_echoed(state, capsys):
    seed(state)
    warm(capsys)
    result = run(capsys, "--predecessor-task-id", ROOT[:12])
    assert result["predecessor_task_id"] == ROOT
    assert result["resolved_from_predecessor_task_id"] == {"input": ROOT[:12], "task_id": ROOT}
    assert len(result["items"]) == 2


def test_no_successors_is_an_empty_scoped_page(state, capsys):
    seed(state)
    warm(capsys)
    result = run(capsys, "--predecessor-task-id", "resume-1")
    assert result["items"] == [] and result["predecessor_task_id"] == "resume-1"
    assert result["successor_scope"] == "recorded_tasks" and result["has_more"] is False


def test_pages_with_a_filter_bound_cursor(state, capsys):
    seed(state)
    warm(capsys)
    first = run(capsys, "--predecessor-task-id", ROOT, "--limit", "1")
    assert [item["task_id"] for item in first["items"]] == ["continue-1"] and first["has_more"] is True
    second = run(capsys, "--predecessor-task-id", ROOT, "--limit", "1", "--cursor", first["next_cursor"])
    assert [item["task_id"] for item in second["items"]] == ["resume-1"] and second["has_more"] is False
    refused = run(capsys, "--predecessor-task-id", OTHER, "--limit", "1", "--cursor", first["next_cursor"], expect=1)
    assert "cursor" in refused["message"].lower()


def test_active_only_and_session_filters_combine(state, capsys):
    seed(state)
    warm(capsys)
    active = run(capsys, "--predecessor-task-id", OTHER, "--active-only")
    assert [item["task_id"] for item in active["items"]] == ["other-child"]
    assert run(capsys, "--predecessor-task-id", ROOT, "--active-only")["items"] == []
    assert [item["task_id"] for item in run(capsys, "--predecessor-task-id", ROOT, "--session-id", "s-resume-1")["items"]] == ["resume-1"]


def test_unknown_invalid_and_pending_predecessors(state, capsys, monkeypatch):
    seed(state)
    warm(capsys)
    assert run(capsys, "--predecessor-task-id", "no-such-task", expect=1)["code"] == "unknown_task"
    assert run(capsys, "--predecessor-task-id", "../escape", expect=1)["code"] == "invalid_task_id"
    from harflow_mcp import id_resolution, task_resources, task_supervision
    monkeypatch.setattr(id_resolution, "resolve_task", lambda *a, **k: id_resolution.Resolution("waiting-task-1", None))
    monkeypatch.setattr(task_supervision, "read_launch", lambda *a: {"state": "waiting_for_resources"})
    monkeypatch.setattr(task_resources, "is_pending_kind", lambda receipt: True)
    error = run(capsys, "--predecessor-task-id", "waiting-task-1", expect=1)
    assert error["code"] == "task_pending_launch" and error["data"]["launch_state"] == "waiting_for_resources"


def test_task_ids_conflict_is_a_usage_error(state, capsys):
    seed(state)
    with pytest.raises(SystemExit) as caught:
        ctl.main(["task-list-page", "--predecessor-task-id", ROOT, "--task-ids", OTHER, "--json"])
    assert caught.value.code == 2


def test_agent_and_undecidable_callers_are_refused_before_the_id_is_resolved(state, capsys, monkeypatch):
    seed(state)
    warm(capsys)
    monkeypatch.undo()  # drop the permissive gate; keep the isolated state below
    monkeypatch.setenv("HOME", str(state.parents[1]))
    monkeypatch.setenv("HARFLOW_TASK_ID", "some-agent")
    for target in (ROOT, "no-such-task"):
        error = run(capsys, "--predecessor-task-id", target, expect=1)
        assert error["code"] == "agent_caller"
    monkeypatch.delenv("HARFLOW_TASK_ID")
    monkeypatch.setattr(lineage, "detect_caller_detail", lambda *a, **k: lineage.Detection(None, "the process table is unavailable"))
    assert run(capsys, "--predecessor-task-id", ROOT, expect=1)["code"] == "caller_undecidable"


def test_listing_without_the_flag_keeps_its_authority(state, capsys, monkeypatch):
    seed(state)
    warm(capsys)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: ("agent_caller", "refused"))
    assert len(run(capsys)["items"]) == 5


def test_sparse_match_over_a_large_catalog_is_correct(state, capsys):
    write(state, ROOT)
    for index in range(300):
        write(state, f"filler-{index:03}", started_at=f"2027-01-0{1 + index % 9}T00:00:00+00:00")
    write(state, "only-child", parent_task_id=ROOT, started_at="2026-12-01T00:00:00+00:00")
    warm(capsys)
    started = time.monotonic()
    result = run(capsys, "--predecessor-task-id", ROOT, "--limit", "100")
    elapsed = time.monotonic() - started
    assert [item["task_id"] for item in result["items"]] == ["only-child"] and result["has_more"] is False
    assert elapsed < 30, f"unindexed successor scan took {elapsed:.2f}s"
    print(f"sparse successor scan over 302 records: {elapsed:.3f}s")


def test_mcp_list_task_page_is_unchanged(state, monkeypatch):
    import asyncio
    from harflow_mcp import server
    from harflow_mcp.tasks import TaskRegistry
    seed(state)
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=state))

    async def read():
        page = {}
        for _ in range(10):
            page = await server.list_task_page(predecessor_task_id=ROOT)
            if not page.get("bootstrap_pending"):
                break
        return page
    page = asyncio.run(read())
    assert [item["task_id"] for item in page["items"]] == ["continue-1", "resume-1"]
    assert "successor_scope" not in page and "predecessor_task_id" not in page
