"""Bounded read transports withhold global diagnostics before caller authority."""

import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from harflow_mcp import catalog, ctl, id_resolution, server, store, workflow_inspection, workflow_reads


FOREIGN_ID = "foreign-workflow-run-sentinel"
BLOCKED_NOTE = "foreign blocked source sentinel"
BLOCKED_CODE = "foreign_blocker_code_sentinel"


def raw_state(status):
    return {
        "status": status,
        "source": "caller_authority",
        "reason": "Workflow caller authority is still being checked",
        "pending_records": 7,
        "blocked_records": 3 if status == "blocked" else 0,
        "blocker_types": [BLOCKED_CODE],
        "blocked_sources": [{"workflow_run_id": FOREIGN_ID, "note": BLOCKED_NOTE, "code": BLOCKED_CODE}],
        "future_diagnostic": {"workflow_run_id": FOREIGN_ID, "count": 19},
    }


def assert_safe_pending(result, status, state_key="catalog_state"):
    assert result[state_key] == {key: raw_state(status)[key] for key in ("status", "source", "reason")}
    encoded = json.dumps(result, sort_keys=True)
    for sentinel in (FOREIGN_ID, BLOCKED_NOTE, BLOCKED_CODE):
        assert sentinel not in encoded
    for field in ("blocked_sources", "blocker_types", "pending_records", "blocked_records", "future_diagnostic",
                  "total_active_count", "total_active_root_count", "total_attention_root_count"):
        assert field not in encoded
    if state_key == "index_state":
        assert result == {
            "current": [], "latest": [], "unavailable": [], "index_state": result[state_key],
            "scan": {"examined": 0, "limit": 500, "current_examined": 0, "current_limit": 100, "complete": False},
        }
    elif "events" in result:
        assert result == {
            "task_id": "known-task", "events": [], "has_more": False, "next_cursor": None,
            "indexing": status == "preparing", **safe_page_fields(status),
        }
    else:
        assert result == {
            "items": [], "related_headers": [], "next_cursor": None, "has_more": False,
            **safe_page_fields(status), **({"workflows": []} if "workflows" in result else {}),
        }


def safe_page_fields(status):
    return {
        "catalog_state": {key: raw_state(status)[key] for key in ("status", "source", "reason")},
        "bootstrap_pending": status == "preparing", "history_incomplete": status == "blocked",
        "authority_incomplete": status == "blocked", "counts_complete": False,
        **({"note": raw_state(status)["reason"]} if status == "blocked" else {}),
    }


def install_pending(monkeypatch, directory, status):
    def premature_lookup(*args, **kwargs):
        pytest.fail("Pending authority must return before resolving identifiers or reading requested content")

    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda path: (False, None))
    monkeypatch.setattr(catalog.Catalog, "state", lambda self: raw_state(status))
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(log_dir=directory, state_layout=None))
    monkeypatch.setattr(workflow_reads, "_resolve_run_arguments", premature_lookup)
    monkeypatch.setattr(workflow_reads, "_perform", premature_lookup)
    monkeypatch.setattr(id_resolution, "resolve_task", premature_lookup)
    monkeypatch.setattr(id_resolution, "resolve_run", premature_lookup)
    monkeypatch.setattr(store, "read", premature_lookup)


@pytest.mark.parametrize("status", ["blocked", "preparing"])
@pytest.mark.parametrize("saved", [False, True])
def test_page_indexing_response_redacts_without_mutating_catalog_state(tmp_path, monkeypatch, status, saved):
    directory = tmp_path / "tasks"
    state = raw_state(status)
    current = {**raw_state("preparing"), "status": "ready"} if saved else state
    original = copy.deepcopy((current, state))
    monkeypatch.setattr(catalog.Catalog, "state", lambda self: current)
    monkeypatch.setattr(catalog, "get_authority_preparation", lambda path: state if saved else None)
    result = workflow_inspection.page_indexing_response(directory)
    assert_safe_pending(result, status)
    assert (current, state) == original


def test_ready_catalog_with_unknown_ownership_stays_neutral_and_does_not_mutate_state(tmp_path, monkeypatch):
    state = {**raw_state("preparing"), "status": "ready"}
    original = copy.deepcopy(state)
    monkeypatch.setattr(catalog.Catalog, "state", lambda self: state)
    monkeypatch.setattr(catalog, "get_authority_preparation", lambda path: None)
    result = workflow_inspection.page_indexing_response(tmp_path / "tasks")
    assert result["catalog_state"] == {
        "status": "preparing", "source": "caller_authority",
        "reason": "Validating workflow caller ownership after metadata indexing",
    }
    assert state == original
    assert set(result["catalog_state"]) == {"status", "source", "reason"}
    assert result["items"] == [] and result["counts_complete"] is False


@pytest.mark.parametrize("status", ["blocked", "preparing"])
@pytest.mark.parametrize("action,kwargs", [
    ("list_run_page", {}),
    ("get_run_header", {"run_id": FOREIGN_ID}),
    ("status", {"run_id": FOREIGN_ID, "_bounded_read": True}),
    ("detail", {"run_id": FOREIGN_ID, "view": "definition", "_bounded_read": True}),
    ("list", {"_bounded_read": True}),
    ("limit_hits", {}),
])
def test_shared_workflow_reads_redact_pending_authority(action, kwargs, status, tmp_path, monkeypatch):
    directory = tmp_path / "tasks"
    install_pending(monkeypatch, directory, status)
    result = asyncio.run(workflow_reads.call(action, directory=directory, **kwargs))
    assert_safe_pending(result, status, "index_state" if action == "limit_hits" else "catalog_state")


@pytest.mark.parametrize("status", ["blocked", "preparing"])
@pytest.mark.parametrize("entry", ["workflow_page", "task_page", "task_event", "workflow_limit_hits"])
async def test_mcp_bounded_read_entry_points_redact_pending_authority(tmp_path, monkeypatch, status, entry):
    directory = tmp_path / "tasks"
    install_pending(monkeypatch, directory, status)
    operations = {
        "workflow_page": server.list_workflow_run_page,
        "task_page": server.list_task_page,
        "task_event": lambda: server.get_task_event_page("known-task"),
        "workflow_limit_hits": server.workflow_limit_hits,
    }
    result = await operations[entry]()
    assert_safe_pending(result, status, "index_state" if entry == "workflow_limit_hits" else "catalog_state")


@pytest.mark.parametrize("status", ["blocked", "preparing"])
@pytest.mark.parametrize("args", [
    ["workflow-list-page", "--json"],
    ["workflow-list", "--monitor-view", "--json"],
    ["workflow-status", "known-run", "--monitor-view", "--json"],
    ["workflow-detail", "known-run", "--monitor-view", "--view", "on_demand_state", "--json"],
    ["workflow-limit-hits", "--json"],
    ["task-list-page", "--json"],
    ["task-list-compact", "--json"],
])
def test_ctl_bounded_read_entry_points_redact_pending_authority(args, status, tmp_path, monkeypatch, capsys):
    directory = tmp_path / "tasks"
    install_pending(monkeypatch, directory, status)
    monkeypatch.setattr(ctl, "default_log_dir", lambda: directory)
    monkeypatch.setattr(catalog.Catalog, "ready", lambda self: False)
    monkeypatch.setattr(store, "bootstrap_catalog", lambda *a, **k: None)
    assert ctl.main(args) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert_safe_pending(result, status, "index_state" if "index_state" in result else "catalog_state")


@pytest.mark.parametrize("status", ["blocked", "preparing"])
async def test_established_direct_reader_preserves_global_diagnostics(tmp_path, monkeypatch, status):
    directory = tmp_path / "tasks"
    original = {"items": [], "catalog_state": raw_state(status), "total_active_count": 23}
    expected = copy.deepcopy(original)
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda path: (True, None))

    async def data_reader(action, selected_directory, managed, kwargs):
        assert action == "list_run_page" and selected_directory == directory and managed is None
        return original

    monkeypatch.setattr(workflow_reads, "_perform", data_reader)
    assert await workflow_reads.call("list_run_page", directory=directory) == expected
    assert original == expected
