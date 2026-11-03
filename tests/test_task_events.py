"""`harflow-ctl task-events`: bidirectional bounded pages, lifecycle evidence and refusals."""
from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from conftest import ALIVE_OWNER

from harflow_mcp import ctl, event_history, events, event_pages, server, state_ownership, store, task_event_read, takeover
from harflow_mcp.events import EVENT_MAX_BYTES
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import PRODUCT_VERSION

TASK = "task-events-1"
PAGE_GOLDEN = "70f448bc5547f976dbcfd3fe4c2db2baa59a4b65b01fed4e23d06f57ad3350f3"
EVENTS_GOLDEN = "44c9b45c34ee9fb0775f0a740fa49aac3a465a58c9d516a1c23dc9fc0b3cf1f1"


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield layout


def record(**overrides) -> store.TaskRecord:
    base = {"task_id": TASK, "backend": "claude", "session_id": "s1", "markers": ["s1"], "repo_path": "/repo",
            "started_at": "2027-01-01T00:00:00+00:00", "status": "completed", "exit_code": 0,
            "finished_at": "2027-01-01T00:00:10+00:00", "output_drained": True, "owner": ALIVE_OWNER}
    return store.TaskRecord(**(base | overrides))


def line(seq, kind="assistant_text", **fields) -> bytes:
    document = {"product": "harflow", "format": "harflow.event", "v": 1, "product_version": PRODUCT_VERSION,
                "seq": seq, "kind": kind, "observed_at": "2027-01-01T00:00:00+00:00", "task_id": TASK}
    if kind == "assistant_text":
        document["text"] = f"text {seq}"
    return (json.dumps(document | fields) + "\n").encode()


def lines(count, start=0, kind="assistant_text") -> bytes:
    return b"".join(line(i, kind) for i in range(start, start + count))


def log_path(state) -> Path:
    return state.tasks_directory / f"{TASK}.events.jsonl"


def seed(state, payload=b"", **overrides) -> Path:
    store.write(state.tasks_directory, record(**overrides))
    path = log_path(state)
    if payload is not None:
        path.write_bytes(payload)
    return path


def run(args, capsys, *, expect=0):
    code = ctl.main(["task-events", TASK, *args, "--json"])
    captured = capsys.readouterr()
    assert code == expect, captured.err
    output = captured.out
    assert len(output.splitlines()) == 1
    document = json.loads(output)
    assert len(output.encode()) <= task_event_read.DOCUMENT_MAX_BYTES
    return document["result"] if expect == 0 else document["error"]


def seqs(result):
    return [event["seq"] for event in result["events"]]


def test_newest_page_is_ascending_and_stops_at_the_last_complete_line(state, capsys):
    seed(state, lines(5) + b'{"seq": 5')
    page = run([], capsys)
    assert page["order"] == "ascending" and page["kinds"] == "default"
    assert seqs(page) == [0, 1, 2, 3, 4]
    assert page["has_older"] is False and page["older_cursor"] is None and page["has_newer"] is False
    assert page["span"] == {"first_seq": 0, "last_seq": 4}
    assert page["skipped"] == {"malformed": 0, "oversized": 0} and page["gaps"] == []
    assert page["events"][0]["format"] == "harflow.event"


def test_limit_pages_walk_back_without_overlap_and_forward_again(state, capsys):
    seed(state, lines(25))
    newest = run(["--limit", "10"], capsys)
    assert seqs(newest) == list(range(15, 25)) and newest["has_older"] and not newest["has_newer"]
    middle = run(["--cursor", newest["older_cursor"], "--limit", "10"], capsys)
    assert seqs(middle) == list(range(5, 15)) and middle["has_newer"]
    oldest = run(["--cursor", middle["older_cursor"], "--limit", "10"], capsys)
    assert seqs(oldest) == list(range(0, 5)) and not oldest["has_older"] and oldest["older_cursor"] is None
    # Direction reversal from any page.
    forward = run(["--cursor", middle["newer_cursor"], "--limit", "10"], capsys)
    assert seqs(forward) == list(range(15, 25)) and not forward["has_newer"]
    back_up = run(["--cursor", oldest["newer_cursor"], "--limit", "3"], capsys)
    assert seqs(back_up) == [5, 6, 7] and back_up["has_newer"] and back_up["has_older"]


def test_from_oldest_reads_forward_from_zero(state, capsys):
    seed(state, lines(12))
    first = run(["--from", "oldest", "--limit", "5"], capsys)
    assert seqs(first) == [0, 1, 2, 3, 4] and not first["has_older"] and first["has_newer"]
    second = run(["--cursor", first["newer_cursor"], "--limit", "5"], capsys)
    third = run(["--cursor", second["newer_cursor"], "--limit", "5"], capsys)
    assert seqs(second) == [5, 6, 7, 8, 9] and seqs(third) == [10, 11] and not third["has_newer"]


def test_polling_newer_cursor_sees_appends_and_holds_back_partial_lines(state, capsys):
    path = seed(state, lines(3))
    page = run([], capsys)
    empty = run(["--cursor", page["newer_cursor"]], capsys)
    assert empty["events"] == [] and empty["newer_cursor"] == page["newer_cursor"] and empty["span"] is None
    with path.open("ab") as handle:
        handle.write(line(3) + line(4)[:25])
    grown = run(["--cursor", page["newer_cursor"]], capsys)
    assert seqs(grown) == [3] and grown["has_newer"] is False
    with path.open("ab") as handle:
        handle.write(line(4)[25:])
    finished = run(["--cursor", grown["newer_cursor"]], capsys)
    assert seqs(finished) == [4]
    assert finished["span"] == {"first_seq": 4, "last_seq": 4}


def test_cursors_for_empty_and_short_logs_survive_appends(state, capsys):
    path = seed(state, b"")
    empty = run([], capsys)
    assert empty["events"] == [] and empty["newer_cursor"] and empty["older_cursor"] is None
    path.write_bytes(line(0)[:30])
    short = run(["--cursor", empty["newer_cursor"]], capsys)
    assert short["events"] == []
    path.write_bytes(lines(4))
    grown = run(["--cursor", short["newer_cursor"]], capsys)
    assert seqs(grown) == [0, 1, 2, 3]
    # A cursor issued mid-way through a short partial log also survives completion of the line.
    path.write_bytes(line(0)[:30])
    partial = run([], capsys)
    path.write_bytes(lines(2))
    assert seqs(run(["--cursor", partial["newer_cursor"]], capsys)) == [0, 1]


def test_default_filter_excludes_deltas_but_accounts_for_their_seqs(state, capsys):
    payload = b"".join(line(i, "assistant_delta" if i % 2 else "assistant_text") for i in range(6))
    seed(state, payload)
    default = run([], capsys)
    assert seqs(default) == [0, 2, 4] and default["span"] == {"first_seq": 0, "last_seq": 5} and default["gaps"] == []
    everything = run(["--all-kinds"], capsys)
    assert seqs(everything) == list(range(6)) and everything["kinds"] == "all"
    only = run(["--kinds", "assistant_delta"], capsys)
    assert seqs(only) == [1, 3, 5] and only["kinds"] == ["assistant_delta"]
    # A cursor is bound to its filter.
    error = run(["--cursor", default["newer_cursor"], "--all-kinds"], capsys, expect=1)
    assert error["code"] == "event_cursor_invalid" and error["data"] == {"reload": "newest"}


def test_gaps_report_unreadable_records_and_unexplained_jumps(state, capsys):
    payload = line(0) + line(1) + b"not json\n" + line(3) + line(7) + line(8)
    seed(state, payload)
    page = run([], capsys)
    assert seqs(page) == [0, 1, 3, 7, 8] and page["skipped"]["malformed"] == 1
    assert page["gaps"] == [{"after_seq": 1, "before_seq": 3, "reason": "unreadable_records"},
                            {"after_seq": 3, "before_seq": 7, "reason": "unknown"}]


def test_gap_is_attributed_across_a_page_boundary(state, capsys):
    seed(state, line(0) + line(1) + line(5) + line(6))
    newest = run(["--limit", "2"], capsys)
    assert seqs(newest) == [5, 6] and newest["gaps"] == []
    older = run(["--cursor", newest["older_cursor"], "--limit", "2"], capsys)
    assert seqs(older) == [0, 1] and older["gaps"] == [{"after_seq": 1, "before_seq": 5, "reason": "unknown"}]
    forward = run(["--cursor", older["newer_cursor"], "--limit", "2"], capsys)
    assert forward["gaps"] == [{"after_seq": 1, "before_seq": 5, "reason": "unknown"}]


def test_oversized_decoded_event_is_a_stub_and_whole_line_skip_is_counted(state, capsys):
    big = line(1, wide={f"k{i}": "v" * 100 for i in range(500)})
    seed(state, line(0) + big + line(2))
    page = run([], capsys)
    stub = page["events"][1]
    assert stub["seq"] == 1 and stub["oversized"] is True and stub["truncated"] is True
    assert page["skipped"]["oversized"] == 0
    over = line(1, text="y" * 1_050_000)
    seed(state, line(0) + over + line(2))
    seen, oversized, page = [], 0, run([], capsys)
    for _ in range(6):
        seen = seqs(page) + seen
        oversized += page["skipped"]["oversized"]
        if not page["has_older"]:
            break
        page = run(["--cursor", page["older_cursor"]], capsys)
    assert seen == [0, 2] and oversized >= 1 and not page["has_older"]


def _walk(state, capsys, direction, *extra):
    seen, calls = [], 0
    page = run(["--from", "oldest", *extra] if direction == "oldest" else list(extra), capsys)
    while True:
        seen = seen + seqs(page) if direction == "oldest" else seqs(page) + seen
        calls += 1
        more = page["has_newer" if direction == "oldest" else "has_older"]
        if not more:
            return seen, calls
        page = run(["--cursor", page["newer_cursor" if direction == "oldest" else "older_cursor"], *extra], capsys)
        assert calls < 30


@pytest.mark.parametrize("length", [3 * 1024 * 1024, 3 * event_pages.SCAN_WINDOW - 50, 3 * event_pages.SCAN_WINDOW - 100,
                                    3 * event_pages.SCAN_WINDOW - 150, 3 * event_pages.SCAN_WINDOW - 300,
                                    2 * event_pages.SCAN_WINDOW + 17])
def test_oversized_line_is_crossed_in_both_directions_without_losing_its_neighbours(state, capsys, length):
    """The valid line before the oversized one is 303 bytes: a window that starts inside it must not drop it."""
    short = line(1, pad="s" * (303 - len(line(1)) + 5 + 1 - 1))
    payload = line(0) + short + b"z" * length + b"\n" + line(3)
    seed(state, payload)
    backward, back_calls = _walk(state, capsys, "newest")
    forward, forward_calls = _walk(state, capsys, "oldest")
    assert backward == forward == [0, 1, 3]
    assert back_calls <= 8 and forward_calls <= 8


def test_forward_read_crosses_scan_windows_of_filtered_events(state, capsys):
    deltas = b"".join(line(i, "assistant_delta", pad="p" * 900) for i in range(2000))
    assert len(deltas) > 2 * event_pages.SCAN_WINDOW
    seed(state, deltas + line(2000))
    page, calls = run(["--from", "oldest", "--limit", "200"], capsys), 1
    found = []
    while True:
        found += seqs(page)
        if not page["has_newer"]:
            break
        page = run(["--cursor", page["newer_cursor"], "--limit", "200"], capsys)
        calls += 1
        assert calls < 40
    assert found == [2000]


def test_size_trimming_keeps_every_event_reachable_in_both_directions(state, capsys):
    padded = b"".join(line(i, pad="é" * 4000) for i in range(40))
    seed(state, padded)
    seen, page, pages = [], run(["--limit", "200"], capsys), 1
    while True:
        seen = seqs(page) + seen
        if not page["has_older"]:
            break
        page = run(["--cursor", page["older_cursor"], "--limit", "200"], capsys)
        pages += 1
    assert seen == list(range(40)) and pages > 1
    forward, page = [], run(["--from", "oldest", "--limit", "200"], capsys)
    while True:
        forward += seqs(page)
        if not page["has_newer"]:
            break
        page = run(["--cursor", page["newer_cursor"], "--limit", "200"], capsys)
    assert forward == list(range(40))


def test_whole_document_bound_with_stubs_gaps_and_longest_cursors(state, capsys):
    near_limit = EVENT_MAX_BYTES - 700
    events = [line(i * 2, pad="q" * (near_limit - len(line(i * 2)))) for i in range(200)]
    seed(state, b"".join(events))
    page = run(["--limit", "200"], capsys)
    assert 0 < len(page["events"]) < 200 and len(page["gaps"]) <= 200
    assert len(page["older_cursor"]) <= event_pages.CURSOR_CHARS and len(page["newer_cursor"]) <= event_pages.CURSOR_CHARS


def test_every_oversized_stub_page_fits(state, capsys):
    stub_lines = [line(i, text="s" * 20_000, pad=["x" * 1900] * 40) for i in range(200)]
    seed(state, b"".join(stub_lines))
    page = run(["--limit", "200"], capsys)
    assert page["events"] and all(event.get("oversized") for event in page["events"])


def test_cursor_validation_precedes_log_reads_and_codes_are_stable(state, capsys):
    seed(state, lines(5))
    page = run(["--limit", "2"], capsys)
    for bad in ("", "x" * 3000, "!!!", base64.urlsafe_b64encode(b"[]").decode(),
                base64.urlsafe_b64encode(json.dumps({"v": 1}).encode()).decode()):
        error = run(["--cursor", bad], capsys, expect=1)
        assert error["code"] == "event_cursor_invalid" and error["data"]["reload"] == "newest"
    decoded = json.loads(base64.urlsafe_b64decode(page["older_cursor"]))
    other = base64.urlsafe_b64encode(json.dumps({**decoded, "task": "0" * 64}).encode()).decode()
    assert run(["--cursor", other], capsys, expect=1)["code"] == "event_cursor_invalid"
    unknown_version = base64.urlsafe_b64encode(json.dumps({**decoded, "v": 3}).encode()).decode()
    assert run(["--cursor", unknown_version], capsys, expect=1)["code"] == "event_cursor_invalid"
    other_generation = base64.urlsafe_b64encode(json.dumps({**decoded, "gen": "rs-events-1"}).encode()).decode()
    assert run(["--cursor", other_generation], capsys, expect=1)["code"] == "event_cursor_expired"


def test_replacement_truncation_anchor_change_and_disappearance_expire_cursors(state, capsys):
    # The edit below keeps the file's length and identity, so only the 64-byte boundary anchor can
    # notice it: replacing "text 6" at seq 6 puts the change inside the anchored bytes of the cursor.
    path = seed(state, lines(10))
    page = run(["--limit", "3"], capsys)
    cursor = page["older_cursor"]
    original = path.read_bytes()
    path.write_bytes(original[:20])
    assert run(["--cursor", cursor], capsys, expect=1)["code"] == "event_cursor_expired"
    path.write_bytes(original.replace(b"text 6", b"text X", 1))
    assert run(["--cursor", cursor], capsys, expect=1)["code"] == "event_cursor_expired"
    path.write_bytes(original)
    assert seqs(run(["--cursor", cursor], capsys)) == list(range(7))
    replacement = path.with_name("replacement")
    replacement.write_bytes(original)
    os.replace(replacement, path)
    assert run(["--cursor", cursor], capsys, expect=1)["code"] == "event_cursor_expired"
    path.unlink()
    error = run(["--cursor", cursor], capsys, expect=1)
    assert error["code"] == "event_cursor_expired"


def test_error_precedence_usage_then_authority_then_task_then_cursor(state, capsys, monkeypatch):
    seed(state, lines(2))
    with pytest.raises(SystemExit) as caught:
        ctl.main(["task-events", TASK])
    assert caught.value.code == 2
    capsys.readouterr()
    for arguments in (["--limit", "0"], ["--limit", "201"], ["--cursor", "x", "--from", "oldest"],
                      ["--kinds", "bogus"], ["--kinds", "usage", "--all-kinds"]):
        with pytest.raises(SystemExit) as caught:
            ctl.main(["task-events", TASK, *arguments, "--json"])
        assert caught.value.code == 2
        assert json.loads(capsys.readouterr().out)["error"]["code"] == "usage"
    ids = []

    def forbidden(*args, **kwargs):
        ids.append(args)
        raise AssertionError("a refused caller must not reach id resolution")
    with monkeypatch.context() as refused:
        refused.setattr(takeover, "caller_refusal", lambda *_: ("agent_caller", "an agent"))
        refused.setattr(ctl.id_resolution, "resolve_task", forbidden)
        for task in (TASK, "task-does-not-exist"):
            assert ctl.main(["task-events", task, "--cursor", "garbage", "--json"]) == 1
            error = json.loads(capsys.readouterr().out)["error"]
            assert error["code"] == "agent_caller" and "direct caller" in error["message"]
    assert ids == []
    assert ctl.main(["task-events", "task-does-not-exist", "--cursor", "garbage", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "unknown_task"
    assert ctl.main(["task-events", TASK, "--cursor", "garbage", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "event_cursor_invalid"


def test_prefix_ids_resolve_and_echo_resolved_from(state, capsys):
    full = "0123456789abcdef0123456789abcdef"
    store.write(state.tasks_directory, record(task_id=full))
    (state.tasks_directory / f"{full}.events.jsonl").write_bytes(lines(1).replace(TASK.encode(), full.encode()))
    assert ctl.main(["task-events", full[:12], "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["task_id"] == full and result["resolved_from"]
    assert len(json.dumps(result)) < task_event_read.DOCUMENT_MAX_BYTES


def test_pending_launch_answers_the_status_error(state, capsys, monkeypatch):
    from harflow_mcp import task_resources
    from harflow_mcp import task_supervision
    monkeypatch.setattr(ctl.id_resolution, "resolve_task", lambda *a, **k: ctl.id_resolution.Resolution(TASK, None))
    monkeypatch.setattr(task_supervision, "read_launch", lambda *_: {"state": "waiting"})
    monkeypatch.setattr(task_resources, "is_pending_kind", lambda receipt: True)
    error = run([], capsys, expect=1)
    assert error["code"] == "task_pending_launch" and error["data"]["launch_state"] == "waiting"


def test_only_the_selected_task_log_is_read(state, capsys):
    seed(state, lines(2))
    (state.tasks_directory / "other.events.jsonl").write_bytes(lines(9))
    assert seqs(run([], capsys)) == [0, 1]


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory"])
def test_special_logs_are_unavailable_not_read(state, capsys, kind, tmp_path):
    path = seed(state, None)
    if kind == "symlink":
        target = tmp_path / "elsewhere.jsonl"
        target.write_bytes(lines(3))
        path.symlink_to(target)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.mkdir()
    page = run([], capsys)
    assert page["events"] == [] and page["log"]["state"] == "unavailable"
    assert page["older_cursor"] is None and page["newer_cursor"] is None


def test_missing_log_is_unavailable(state, capsys):
    seed(state, None)
    assert run([], capsys)["log"]["state"] == "unavailable"


# --- lifecycle ---------------------------------------------------------------------------------

def finished(seq):
    return line(seq, "task_finished", status="completed")


def test_final_requires_drained_and_task_finished_at_eof_whatever_the_filter(state, capsys):
    seed(state, lines(3) + finished(3))
    page = run(["--kinds", "usage"], capsys)
    assert page["events"] == [] and page["log"] == {
        "state": "final", "task_status": "completed", "output_drained": True,
        "capture_state": "observed_terminal", "final_seq": 3}
    seed(state, lines(3) + finished(3) + b'{"partial')
    assert run([], capsys)["log"]["state"] != "final"
    seed(state, lines(3) + finished(3) + lines(1, start=4))
    assert run([], capsys)["log"]["final_seq"] is None


def test_settling_then_incomplete_after_the_grace_period(state, capsys, monkeypatch):
    recent = datetime.now(timezone.utc)
    seed(state, lines(3), finished_at=recent.isoformat())
    assert run([], capsys)["log"]["state"] == "settling"
    monkeypatch.setattr(task_event_read, "_now", lambda: recent + timedelta(seconds=task_event_read.SETTLE_GRACE_SECONDS + 1))
    assert run([], capsys)["log"]["state"] == "incomplete"
    seed(state, lines(3), finished_at=recent.isoformat(), output_drained=False)
    assert run([], capsys)["log"]["state"] == "incomplete"


def test_failed_task_finished_write_never_becomes_final(state, capsys, monkeypatch):
    seed(state, lines(3), finished_at="2020-01-01T00:00:00+00:00")
    assert run([], capsys)["log"] == {"state": "incomplete", "task_status": "completed", "output_drained": True,
                                      "capture_state": "observed_terminal", "final_seq": None}


def test_last_line_longer_than_the_window_is_not_provable(state, capsys):
    seed(state, lines(2) + line(2, "task_finished", status="completed", pad="p" * (event_pages.SCAN_WINDOW + 10)),
         finished_at="2020-01-01T00:00:00+00:00")
    page = run([], capsys)
    assert page["log"]["state"] == "incomplete" and page["log"]["final_seq"] is None


def test_open_lost_unknown_states_follow_capture_evidence(state, capsys, identities):
    seed(state, lines(2), status="running", exit_code=None, finished_at=None, output_drained=False)
    assert run([], capsys)["log"]["state"] == "open"
    identities.dead(ALIVE_OWNER["pid"])
    assert run([], capsys)["log"]["state"] == "incomplete"
    identities.verdicts.pop(ALIVE_OWNER["pid"])
    assert run([], capsys)["log"]["state"] == "unknown"
    seed(state, lines(2) + finished(2), status="running", exit_code=None, finished_at=None, output_drained=True)
    identities.dead(ALIVE_OWNER["pid"])
    assert run([], capsys)["log"]["state"] == "incomplete"


def test_change_token_tracks_source_and_lifecycle_not_observation_time(state, capsys, monkeypatch):
    path = seed(state, lines(3))
    first = run([], capsys)
    again = run([], capsys)
    assert first["change_token"] == again["change_token"] and first["observed_at"] <= again["observed_at"]
    with path.open("ab") as handle:
        handle.write(line(3))
    assert run([], capsys)["change_token"] != first["change_token"]
    before = run([], capsys)["change_token"]
    seed(state, path.read_bytes(), finished_at="2020-01-01T00:00:00+00:00")
    assert run([], capsys)["change_token"] != before


# --- MCP and module boundaries -----------------------------------------------------------------

def test_mcp_cursor_reader_output_is_unchanged(state):
    path = state.tasks_directory / "mcp.events.jsonl"
    path.write_bytes(lines(250))
    first = event_history.read_cursor_page(path, layout=state)
    second = event_history.read_cursor_page(path, cursor=first.next_cursor, layout=state)
    assert [event["seq"] for event in first.events] == list(range(150, 250))
    assert [event["seq"] for event in second.events] == list(range(50, 150))
    assert json.loads(base64.b64decode(first.next_cursor, altchars=b"-_"))["v"] == 1
    assert first.has_more and first.snapshot_end == len(path.read_bytes())


def test_mcp_event_tools_do_not_depend_on_the_new_reader():
    import inspect
    source = inspect.getsource(server)
    assert "event_pages" not in source and "task_event_read" not in source


def test_ctl_task_reads_do_not_import_the_server():
    import inspect
    from harflow_mcp import ctl_task_reads
    for module in (ctl_task_reads, task_event_read, event_pages):
        assert "import server" not in inspect.getsource(module) and "from .server" not in inspect.getsource(module)


def test_cursor_of_a_foreign_log_identity_is_expired(state, capsys, monkeypatch):
    seed(state, lines(6))
    page = run(["--limit", "2"], capsys)
    monkeypatch.setattr(event_pages, "identity_of", lambda info: (info.st_dev, info.st_ino + 1))
    assert run(["--cursor", page["older_cursor"]], capsys, expect=1)["code"] == "event_cursor_expired"


async def test_mcp_event_tools_output_is_byte_identical_to_the_pre_ctl_reader(state, monkeypatch):
    """Digests were taken from the MCP tools before `task-events` existed; they must never move."""
    from harflow_mcp import id_resolution, workflow_inspection
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda _: (True, None))
    monkeypatch.setattr(store, "read", lambda *a, **k: object())
    monkeypatch.setattr(server, "_guard_task_read", lambda *a: None)
    monkeypatch.setattr(id_resolution, "resolve_task", lambda log_dir, value, **kw: id_resolution.Resolution(value, None))
    registry = server._reg()
    monkeypatch.setattr(registry, "_log_dir", state.tasks_directory)
    registry.state_layout = state
    payload = b"".join(line(i, "assistant_delta" if i % 7 == 0 else "assistant_text", product_version="0.0.7") for i in range(130))
    (state.tasks_directory / f"{TASK}.events.jsonl").write_bytes(payload + b'{"partial')
    page = await server.get_task_event_page(TASK, limit=20)
    page.pop("next_cursor")
    seq_page = events.read_page(state.tasks_directory / f"{TASK}.events.jsonl", limit=20, layout=state)
    seq_reader = {"events": seq_page.events, "has_more": seq_page.has_more, "before": seq_page.next_before_seq,
                  "after": seq_page.next_after_seq, "skipped": seq_page.skipped_oversized}
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    assert event_pages.digest(canonical(page)) == PAGE_GOLDEN
    assert event_pages.digest(canonical(seq_reader)) == EVENTS_GOLDEN


def test_task_read_fixtures_declare_their_capabilities_and_the_engine_advertises_them():
    from harflow_mcp.wire_contracts import CAPABILITIES
    root = Path(__file__).parents[1] / "contracts" / "v1"
    manifest = json.loads((root / "manifest.json").read_text())
    required = {"ctl-task-events-": "task-event-page.v1", "ctl-task-prompt-": "task-prompt.v1"}
    seen = set()
    for name, case in manifest["cases"].items():
        for prefix, capability in required.items():
            if name.startswith(prefix):
                seen.add(prefix)
                assert case["required_capabilities"] == [capability] and case["expected_product_version"] == PRODUCT_VERSION
                assert capability in CAPABILITIES
    assert seen == set(required)
    advertised = json.loads((root / "version-info.json").read_text())["capabilities"]
    assert advertised == list(CAPABILITIES)


# --- review round 1 ------------------------------------------------------------------------------

def encode_cursor(value):
    return base64.urlsafe_b64encode(json.dumps(value).encode()).decode()


def test_malformed_cursor_fields_are_refused_as_invalid(state, capsys):
    seed(state, lines(5))
    decoded = json.loads(base64.urlsafe_b64decode(run(["--limit", "2"], capsys)["older_cursor"]))
    mutations = [{"dir": []}, {"dir": {}}, {"dir": 3}, {"resync": "no"}, {"skip": 1}, {"gen": []}, {"seq": "1"},
                 {"seq": -1}, {"boundary": True}, {"hlen": 65}, {"anchor": 1}]
    for change in mutations:
        error = run(["--cursor", encode_cursor({**decoded, **change})], capsys, expect=1)
        assert error["code"] == "event_cursor_invalid", change
    for missing in ("seq", "skip", "dir", "resync", "gen", "hlen"):
        value = {key: item for key, item in decoded.items() if key != missing}
        assert run(["--cursor", encode_cursor(value)], capsys, expect=1)["code"] == "event_cursor_invalid", missing


def test_open_task_poll_does_not_replay_the_raw_stream(state, capsys, identities, monkeypatch):
    seed(state, lines(2), status="running", exit_code=None, finished_at=None, output_drained=False)
    monkeypatch.setattr(store, "replay_log", lambda *a, **k: pytest.fail("the raw stream was replayed"))
    page = run([], capsys)
    assert page["log"]["state"] == "open" and page["log"]["task_status"] == "running"


def test_deeply_nested_final_line_gives_no_finality_evidence(state, capsys):
    nested = b'{"a":' * 1100 + b"1" + b"}" * 1100 + b"\n"
    seed(state, lines(2) + nested, finished_at=datetime.now(timezone.utc).isoformat())
    assert run([], capsys)["log"]["state"] == "settling"
    seed(state, lines(2) + nested, finished_at="2020-01-01T00:00:00+00:00")
    assert run([], capsys)["log"]["state"] == "incomplete"


def test_finished_at_in_the_future_is_not_a_grace_period(state, capsys):
    seed(state, lines(2), finished_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())
    assert run([], capsys)["log"]["state"] == "incomplete"


def test_task_finished_that_never_reached_the_log_settles_then_is_incomplete(state, capsys, monkeypatch):
    now = datetime.now(timezone.utc)
    seed(state, lines(4), finished_at=now.isoformat(), output_drained=True)
    assert run([], capsys)["log"] == {"state": "settling", "task_status": "completed", "output_drained": True,
                                     "capture_state": "observed_terminal", "final_seq": None}
    monkeypatch.setattr(task_event_read, "_now", lambda: now + timedelta(seconds=300))
    assert run([], capsys)["log"]["state"] == "incomplete"


def test_skipped_lines_at_a_page_boundary_are_attributed_in_both_directions(state, capsys):
    seed(state, line(0) + line(1) + b"garbage\n" + line(3) + line(4))
    newest = run(["--limit", "2"], capsys)
    assert seqs(newest) == [3, 4] and newest["gaps"] == []
    older = run(["--cursor", newest["older_cursor"], "--limit", "3"], capsys)
    assert seqs(older) == [0, 1] and older["gaps"] == [{"after_seq": 1, "before_seq": 3, "reason": "unreadable_records"}]
    first = run(["--from", "oldest", "--limit", "2"], capsys)
    # The skipped line is consumed with the next forward page: its boundary gap is attributed too.
    second = run(["--cursor", first["newer_cursor"], "--limit", "3"], capsys)
    assert seqs(second) == [3, 4] and second["gaps"] == [{"after_seq": 1, "before_seq": 3, "reason": "unreadable_records"}]
    # An empty page keeps the bit for the page after it.
    path = log_path(state)
    seed(state, line(0) + b"garbage\n" + line(2))
    one = run(["--from", "oldest", "--limit", "1"], capsys)
    skip_only = run(["--cursor", one["newer_cursor"], "--limit", "1"], capsys)
    assert skip_only["events"] == [] and skip_only["skipped"]["malformed"] == 1
    after = run(["--cursor", skip_only["newer_cursor"], "--limit", "1"], capsys)
    assert after["gaps"] == [{"after_seq": 0, "before_seq": 2, "reason": "unreadable_records"}] and path.exists()


def test_a_partial_trailing_line_is_not_a_newer_line_after_reversal(state, capsys):
    seed(state, lines(6) + b'{"partial')
    newest = run(["--limit", "3"], capsys)
    older = run(["--cursor", newest["older_cursor"], "--limit", "3"], capsys)
    assert older["has_newer"] is True
    forward = run(["--cursor", older["newer_cursor"], "--limit", "3"], capsys)
    assert seqs(forward) == [3, 4, 5] and forward["has_newer"] is False
    # A cursor at the very end of the complete lines has nothing complete after it.
    reverse = run(["--cursor", forward["older_cursor"], "--limit", "1"], capsys)
    assert reverse["has_newer"] is True
    tail_only = seed(state, lines(2) + b'{"partial')
    page = run(["--limit", "1"], capsys)
    assert seqs(page) == [1] and page["has_newer"] is False and tail_only.exists()


def test_empty_older_page_reports_the_issued_boundary(state, capsys):
    path = seed(state, lines(3))
    newest = run(["--limit", "1"], capsys)
    cursor = json.loads(base64.urlsafe_b64decode(newest["older_cursor"]))
    # A mid-line boundary inside an unterminated first line: nothing complete lies before it.
    path.write_bytes(line(0)[:50] + b"x" * 40)
    anchor_length = 64
    payload = path.read_bytes()
    boundary = len(payload)
    import hashlib
    forged = {**cursor, "boundary": boundary, "resync": True, "seq": None, "skip": False,
              "anchor": hashlib.sha256(payload[max(0, boundary - anchor_length):boundary]).hexdigest(),
              "head": hashlib.sha256(payload[:min(anchor_length, boundary)]).hexdigest(), "hlen": min(anchor_length, boundary)}
    page = run(["--cursor", encode_cursor(forged)], capsys)
    assert page["events"] == [] and page["older_cursor"] is None
    issued = json.loads(base64.urlsafe_b64decode(page["newer_cursor"]))
    assert issued["boundary"] == boundary and issued["resync"] is True


def test_forward_then_reverse_on_a_trimmed_page(state, capsys):
    seed(state, b"".join(line(i, pad="é" * 4000) for i in range(40)))
    first = run(["--from", "oldest", "--limit", "200"], capsys)
    assert 0 < len(first["events"]) < 40 and first["has_newer"]
    second = run(["--cursor", first["newer_cursor"], "--limit", "200"], capsys)
    assert seqs(second)[0] == seqs(first)[-1] + 1
    back = run(["--cursor", second["older_cursor"], "--limit", "200"], capsys)
    assert seqs(back)[-1] == seqs(first)[-1] and seqs(back)[0] >= 0 and back["has_newer"]
    again = run(["--cursor", back["newer_cursor"], "--limit", "200"], capsys)
    assert seqs(again) == seqs(second)


def test_managed_callers_are_confined_by_the_status_read_guard(state, capsys, monkeypatch):
    from harflow_mcp import workflow_inspection
    store.write(state.tasks_directory, record(task_id="owned", prompt="p"))
    (state.tasks_directory / "owned.events.jsonl").write_bytes(lines(2).replace(TASK.encode(), b"owned"))
    store.write(state.tasks_directory, record(task_id="foreign"))
    managed = ({"role": "node", "activation_id": "exec"},
               {"workflow_run_id": "run", "activations": [{"id": "exec", "role": "node", "status": "running",
                                                          "tasks": [{"task_id": "owned", "status": "running"}]}]})
    monkeypatch.setattr(workflow_inspection, "managed_reader", lambda log_dir: managed)
    for command, tail in (("task-events", []), ("task-prompt", [])):
        assert ctl.main([command, "owned", *tail, "--json"]) == 0
        capsys.readouterr()
        for other in ("foreign", "missing"):
            assert ctl.main([command, other, *tail, "--json"]) == 1
            assert json.loads(capsys.readouterr().out)["error"]["code"] == "workflow_read_refused"


def test_workflow_native_execution_ids_are_unknown_tasks(state, capsys):
    seed(state, lines(1))
    for command in ("task-events", "task-prompt"):
        assert ctl.main([command, "execution:node-1:attempt-2", "--json"]) == 1
        assert json.loads(capsys.readouterr().out)["error"]["code"] == "invalid_task_id"
        assert ctl.main([command, "native-execution-1", "--json"]) == 1
        assert json.loads(capsys.readouterr().out)["error"]["code"] == "unknown_task"


@pytest.mark.parametrize("tail", [65_535, 65_536, 65_537])
def test_has_newer_after_an_older_cursor_needs_a_newline(state, capsys, tail):
    seed(state, lines(2) + b"x" * tail)
    newest = run(["--limit", "1"], capsys)
    assert seqs(newest) == [1]
    older = run(["--cursor", newest["older_cursor"], "--limit", "1"], capsys)
    assert seqs(older) == [0] and older["has_newer"] is True  # line 1 is complete after it
    # Past the only complete line there are only unterminated bytes.
    forward = run(["--cursor", older["newer_cursor"], "--limit", "1"], capsys)
    reverse = run(["--cursor", forward["older_cursor"], "--limit", "1"], capsys)
    assert seqs(reverse) == [0] and reverse["has_newer"] is True
    end = json.loads(base64.urlsafe_b64decode(forward["newer_cursor"]))
    behind = run(["--cursor", encode_cursor({**end, "dir": "older"}), "--limit", "1"], capsys)
    assert behind["has_newer"] is False


def test_has_newer_sees_a_complete_line_longer_than_64_kib(state, capsys):
    seed(state, lines(2) + line(2, pad="p" * 70_000) + b"y" * 10)
    older = run(["--cursor", run(["--limit", "1"], capsys)["older_cursor"], "--limit", "1"], capsys)
    assert seqs(older) == [1] and older["has_newer"] is True
