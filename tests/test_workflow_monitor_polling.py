import json

import pytest

from polybridge.workflow_responses import BUDGET, detail, monitor


def test_selected_monitor_projection_bounds_large_results_and_history():
    run = {"workflow_run_id": "run", "status": "running", "definition": {"instructions": "x" * 1_000_000},
           "activations": [{"id": str(i), "node_id": "review", "status": "completed", "node_result": {"result": "y" * 100_000}} for i in range(300)],
           "technical_plan": "z" * 1_000_000}
    response = monitor(run)
    assert len(json.dumps(response).encode()) < BUDGET
    assert "activations" not in response
    assert "definition" not in response
    assert len(response["monitor_digests"]["execution_index"]) == 64
    assert detail(run, "execution:299")["total_characters"] > 100_000


def test_index_digests_isolate_changed_execution_without_retransferring_old_results():
    run = {"workflow_run_id": "run", "activations": [{"id": "a", "result": "old"}, {"id": "b", "result": "new"}]}
    before = json.loads(detail(run, "execution_index")["chunk"])
    run["activations"][1]["result"] = "updated"
    after = json.loads(detail(run, "execution_index")["chunk"])
    assert before[0] == after[0]
    assert before[1]["digest"] != after[1]["digest"]


def test_execution_chunks_lossless_and_stale_cursor_rejected():
    run = {"workflow_run_id": "run", "activations": [{"id": "a", "result": "😀" * 20_000}]}
    first = detail(run, "execution:a")
    cursor = first["next_cursor"]
    text = first["chunk"]
    while cursor:
        page = detail(run, "execution:a", cursor)
        text += page["chunk"]
        cursor = page["next_cursor"]
    assert json.loads(text) == run["activations"][0]
    run["activations"][0]["result"] = "changed"
    with pytest.raises(ValueError, match="stale"):
        detail(run, "execution:a", first["next_cursor"])


def test_builder_projection_preserves_revision_and_lossless_source_views():
    run = {"workflow_run_id": "builder", "kind": "builder", "draft_revision": 42,
           "source_revision": 12, "source_name": "A", "builder_draft": {"instructions": "x" * 50_000},
           "source_saved_definition": {"instructions": "y" * 50_000}}
    response = monitor(run)
    assert response["draft_revision"] == 42
    assert response["source_revision"] == 12
    assert detail(run, "source_saved_definition")["total_characters"] > 50_000


def test_monitor_unicode_metadata_cannot_defeat_transport_budget():
    run = {key: "😀" * 10_000 for key in ("workflow_run_id", "name", "reason", "summary", "failure_reason", "attention_reason", "wait_reason", "input_question", "source_name", "workflow_name")}
    run["activations"] = []
    assert len(json.dumps(monitor(run)).encode()) < BUDGET


def test_monitor_cli_surface_is_additive_and_ordinary_status_unchanged():
    from polybridge.ctl import _build_parser
    parser = _build_parser()[0]
    ordinary = parser.parse_args(["workflow-status", "run"])
    selected = parser.parse_args(["workflow-status", "run", "--monitor-view"])
    paged = parser.parse_args(["workflow-detail", "run", "--view=execution:a", "--cursor=opaque"])
    assert ordinary.monitor_view is False
    assert selected.monitor_view is True
    assert paged.view == "execution:a"
    assert paged.cursor == "opaque"
