"""`harflow-ctl task-changes`: usage errors, the direct-caller gate, id resolution and the stable refusals."""
from __future__ import annotations

import base64
import json

import pytest

from conftest import ALIVE_OWNER
from task_changes_support import TASK, make_document, make_repository, make_row, write_document

from harflow_mcp import ctl, id_resolution, state_ownership, store, takeover, task_resources, task_supervision
from harflow_mcp import task_changes_document as shape
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CAPABILITIES


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    for key in ("HARFLOW_STATE_ROOT", "HARFLOW_TASK_ID", "HARFLOW_DEPTH"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield layout


def seed(state, task_id=TASK, **overrides):
    values = dict(task_id=task_id, backend="claude", session_id="s1", markers=["s1"], repo_path="/repo",
                  started_at="2027-01-01T00:00:00+00:00", status="completed", exit_code=0,
                  finished_at="2027-01-01T00:00:10+00:00", owner=ALIVE_OWNER, outcome={"repositories": [{"repository_root": "/r"}]})
    store.write(state.tasks_directory, store.TaskRecord(**(values | overrides)))


def run(capsys, *args, expect=0, task=TASK):
    code = ctl.main(["task-changes", task, *args, "--json"])
    captured = capsys.readouterr()
    assert code == expect, captured.err
    assert len(captured.out.splitlines()) == 1
    document = json.loads(captured.out)
    return document["result"] if expect == 0 else document["error"]


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode()


def document(state, rows=None):
    rows = rows if rows is not None else [make_row("a.txt", b"diff a\n"), make_row("b.txt", b"diff b\n")]
    write_document(state.tasks_directory, make_document([make_repository("/r", rows)]))


def test_capability_is_declared():
    assert "task-changes.v1" in CAPABILITIES
    assert CAPABILITIES[-1] == "task-list-watch.v1"


def test_listing_text_and_continuation_through_ctl(state, capsys):
    seed(state)
    document(state)
    page = run(capsys, "--limit", "1")
    assert page["mode"] == "list" and page["has_more"] is True and page["capture"] == {"state": "available", "reason": None}
    again = run(capsys, "--cursor", page["next_cursor"])
    assert [row["path"] for row in again["repositories"][0]["scopes"]["committed_range"]["rows"]] == ["b.txt"]
    text = run(capsys, "--repository-root", "/r", "--scope", "committed_range", "--path-b64", b64(b"a.txt"))
    assert text["mode"] == "text" and text["text"] == "diff a\n"


@pytest.mark.parametrize("arguments", [
    ["--repository-root", "/r"],
    ["--repository-root", "/r", "--scope", "working_tree"],
    ["--scope", "working_tree", "--path-b64", "YQ=="],
    ["--repository-root", "/r", "--scope", "working_tree", "--path-b64", "YQ==", "--cursor", "x"],
    ["--repository-root", "/r", "--scope", "working_tree", "--path-b64", "YQ==", "--limit", "5"],
    ["--repository-root", "/r", "--scope", "working_tree", "--path-b64", "not base64!"],
    ["--scope", "elsewhere", "--repository-root", "/r", "--path-b64", "YQ=="],
    ["--limit", "0"], ["--limit", "501"],
])
def test_usage_errors_precede_everything(state, capsys, monkeypatch, arguments):
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: ("agent_caller", "an agent"))
    with pytest.raises(SystemExit) as caught:
        ctl.main(["task-changes", "no-such-task", *arguments, "--json"])
    assert caught.value.code == 2
    out = capsys.readouterr().out  # argparse's own `choices` refusal is plain text on stderr, as for every other command
    assert (out == "" and "elsewhere" in arguments) or json.loads(out)["error"]["code"] == "usage"
    with pytest.raises(SystemExit):
        ctl.main(["task-changes", TASK])
    capsys.readouterr()


def test_refused_callers_learn_nothing_about_ids(state, capsys, monkeypatch):
    seed(state)
    document(state)
    for refusal in (("agent_caller", "an agent"), ("caller_undecidable", "no process table")):
        with monkeypatch.context() as refused:
            refused.setattr(takeover, "caller_refusal", lambda *_, value=refusal: value)
            for task in (TASK, "task-does-not-exist"):
                error = run(capsys, expect=1, task=task)
                assert error["code"] == refusal[0] and "task-does-not-exist" not in error["message"]


def test_prefix_ids_echo_resolution_and_unknown_task(state, capsys):
    full = "0123456789abcdef0123456789abcdef"
    seed(state, task_id=full)
    assert run(capsys, task=full[:10])["resolved_from"]["task_id"] == full
    assert run(capsys, expect=1, task="task-does-not-exist")["code"] == "unknown_task"


def test_pending_launch_id(state, capsys, monkeypatch):
    monkeypatch.setattr(id_resolution, "resolve_task", lambda *a, **k: id_resolution.Resolution("pending-task", None))
    monkeypatch.setattr(task_supervision, "read_launch", lambda *a: {"state": "waiting_for_resources"})
    monkeypatch.setattr(task_resources, "is_pending_kind", lambda receipt: True)
    error = run(capsys, expect=1, task="pending-task")
    assert error["code"] == "task_pending_launch" and error["data"] == {"launch_state": "waiting_for_resources"}


def test_stable_refusal_codes(state, capsys):
    seed(state)
    document(state)
    cursor = run(capsys, "--limit", "1")["next_cursor"]
    invalid = run(capsys, "--cursor", "not-a-cursor", expect=1)
    assert invalid["code"] == "changes_cursor_invalid"
    document(state, [make_row("only.txt", b"x")])
    expired = run(capsys, "--cursor", cursor, expect=1)
    assert expired["code"] == "changes_cursor_expired" and expired["data"] == {"reload": "first"}
    missing = run(capsys, "--repository-root", "/r", "--scope", "working_tree", "--path-b64", b64(b"only.txt"), expect=1)
    assert missing["code"] == "changes_entry_not_found"


def test_capture_states_without_a_document(state, capsys):
    seed(state, status="running", exit_code=None, finished_at=None)
    assert run(capsys)["capture"] == {"state": "pending", "reason": "task_running"}
    seed(state, status="failed", exit_code=None)
    assert run(capsys)["capture"] == {"state": "not_captured", "reason": "outcome_unobserved"}
    seed(state, outcome=None)
    assert run(capsys)["capture"] == {"state": "available", "reason": "no_repository"}
    seed(state, finished_at="2027-01-01T00:00:10+00:00")
    assert shape.SUFFIX and run(capsys)["capture"]["reason"] == "capture_missing"


def test_page_too_large_is_a_stable_refusal(state, capsys, monkeypatch):
    from harflow_mcp import task_changes_read
    seed(state)
    document(state)
    monkeypatch.setattr(task_changes_read, "DOCUMENT_MAX_BYTES", 300)
    assert run(capsys, expect=1)["code"] == "changes_page_too_large"
