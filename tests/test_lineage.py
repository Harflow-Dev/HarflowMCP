"""Best-effort caller detection: PB_TASK_ID, then session match, then ancestry walk.

Every `detect_caller` call here injects `environ`/`getsid`/`getpid`/`process_table`/`check`, so
none of it touches the real process tree — records are written to `tmp_path` via `store.write`,
never to `~/.polybridge`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import lineage, store
# Captured at import time, before the autouse `no_caller_detected` fixture in conftest.py
# monkeypatches `lineage.detect_caller` to a stub that always returns None — this module tests
# the real thing, so every call below goes through this name rather than the module attribute.
from polybridge.lineage import detect_caller as real_detect_caller
from polybridge.store import TaskRecord

NOW = datetime(2026, 9, 24, 12, 0, 0, tzinfo=timezone.utc)


def make_record(tmp_path: Path, task_id: str, **overrides) -> TaskRecord:
    base = {
        "task_id": task_id,
        "backend": "claude",
        "session_id": None,
        "repo_path": str(tmp_path),
        "started_at": NOW.isoformat(),
        "pid": 4242,
        "pgid": 4242,
        "start_time": "Thu Sep 24 12:00:00 2026",
        "markers": [],
        "status": "running",
        "exit_code": None,
    }
    record = TaskRecord(**(base | overrides))
    store.write(tmp_path, record)
    return record


def check_by_pid(mapping: dict[int, str], default: str = "dead"):
    """A `check` stub that reports a fixed verdict per pid, rather than shelling out to `ps`."""

    def check(identity):
        return mapping.get(identity.get("pid"), default)

    return check


def always(verdict: str):
    return lambda identity: verdict


def raising_getsid(pid: int) -> int:
    raise OSError("no session for this process")


# --- PB_TASK_ID (method 1) ----------------------------------------------------------------------


def test_pb_task_id_confirmed_by_session_match(tmp_path: Path) -> None:
    make_record(tmp_path, "aaaaaaaa", pid=100, pgid=555)

    caller = real_detect_caller(
        tmp_path,
        environ={"PB_TASK_ID": "aaaaaaaa"},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "pb_task_id"
    assert caller.record.task_id == "aaaaaaaa"


def test_pb_task_id_confirmed_by_ancestry(tmp_path: Path) -> None:
    make_record(tmp_path, "bbbbbbbb", pid=100, pgid=None)
    table = {999: 500, 500: 100, 100: 1}

    caller = real_detect_caller(
        tmp_path,
        environ={"PB_TASK_ID": "bbbbbbbb"},
        getsid=lambda pid: 777,  # matches nobody's pgid
        getpid=lambda: 999,
        process_table=lambda: table,
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "pb_task_id"
    assert caller.record.task_id == "bbbbbbbb"


def test_pb_task_id_naming_a_dead_record_falls_through(tmp_path: Path) -> None:
    make_record(tmp_path, "cccccccc", pid=100, pgid=100)
    make_record(tmp_path, "dddddddd", pid=200, pgid=555)

    caller = real_detect_caller(
        tmp_path,
        environ={"PB_TASK_ID": "cccccccc"},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=check_by_pid({100: "dead", 200: "alive"}),
    )

    assert caller is not None
    assert caller.method == "session"
    assert caller.record.task_id == "dddddddd"


def test_pb_task_id_naming_an_undecidable_record_falls_through(tmp_path: Path) -> None:
    make_record(tmp_path, "eeeeeeee", pid=100, pgid=None)
    table = {999: 300, 300: 1}
    make_record(tmp_path, "ffffffff", pid=300, pgid=None)

    caller = real_detect_caller(
        tmp_path,
        environ={"PB_TASK_ID": "eeeeeeee"},
        getsid=lambda pid: 1,
        getpid=lambda: 999,
        process_table=lambda: table,
        check=check_by_pid({100: "undecidable", 300: "alive"}),
    )

    assert caller is not None
    assert caller.method == "ancestry"
    assert caller.record.task_id == "ffffffff"


def test_pb_task_id_with_invalid_chars_is_ignored(tmp_path: Path) -> None:
    make_record(tmp_path, "gggggggg", pid=100, pgid=555)

    caller = real_detect_caller(
        tmp_path,
        environ={"PB_TASK_ID": "../etc/passwd"},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    # Falls straight through to the session method rather than raising or matching by accident —
    # the invalid value never even reaches a record lookup.
    assert caller is not None
    assert caller.method == "session"
    assert caller.record.task_id == "gggggggg"


def test_stale_pb_task_id_unrelated_to_this_process_falls_through(tmp_path: Path) -> None:
    make_record(tmp_path, "hhhhhhhh", pid=100, pgid=42)  # alive, but wrong session and no ancestry
    make_record(tmp_path, "iiiiiiii", pid=200, pgid=555)

    caller = real_detect_caller(
        tmp_path,
        environ={"PB_TASK_ID": "hhhhhhhh"},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},  # empty: no ancestry relation to "hhhhhhhh" either
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "session"
    assert caller.record.task_id == "iiiiiiii"


# --- Session match (method 2) -------------------------------------------------------------------


def test_session_method_picks_the_record_sharing_our_session(tmp_path: Path) -> None:
    make_record(tmp_path, "jjjjjjjj", pid=100, pgid=42)  # different session
    make_record(tmp_path, "kkkkkkkk", pid=200, pgid=555)

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "session"
    assert caller.record.task_id == "kkkkkkkk"


def test_session_method_prefers_the_latest_started_at_when_several_match(tmp_path: Path) -> None:
    make_record(
        tmp_path, "older", pid=100, pgid=555,
        started_at=(NOW - timedelta(minutes=5)).isoformat(),
    )
    make_record(tmp_path, "newer", pid=200, pgid=555, started_at=NOW.isoformat())

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "session"
    assert caller.record.task_id == "newer"


def test_session_method_unavailable_when_getsid_fails(tmp_path: Path) -> None:
    make_record(tmp_path, "llllllll", pid=100, pgid=555)

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=raising_getsid,
        getpid=lambda: 999,
        process_table=lambda: {},  # no ancestry relation either
        check=always("alive"),
    )

    assert caller is None


# --- Ancestry walk (method 3) -------------------------------------------------------------------


def test_ancestry_method_walks_multiple_levels(tmp_path: Path) -> None:
    make_record(tmp_path, "mmmmmmmm", pid=100, pgid=None)
    table = {999: 500, 500: 300, 300: 100, 100: 1}

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 1,  # no session match
        getpid=lambda: 999,
        process_table=lambda: table,
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "ancestry"
    assert caller.record.task_id == "mmmmmmmm"


def test_ancestors_is_cycle_safe() -> None:
    table = {1: 2, 2: 3, 3: 1}  # a cycle, which a real process tree should never have

    assert lineage.ancestors(1, table) == [2, 3]


def test_ancestors_stops_before_0_and_1() -> None:
    table = {10: 5, 5: 1}

    assert lineage.ancestors(10, table) == [5]


# --- Observed-terminal filtering ------------------------------------------------------------


def test_observed_terminal_record_is_never_a_candidate(tmp_path: Path) -> None:
    make_record(tmp_path, "nnnnnnnn", pid=100, pgid=555, status="completed", exit_code=0)

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    assert caller is None


def test_unobserved_terminal_record_is_still_a_candidate(tmp_path: Path) -> None:
    """Terminal status with no exit code means nothing actually saw this run finish — it is still
    fair game for caller detection, mirroring `store.outcome_unobserved`."""
    make_record(tmp_path, "oooooooo", pid=100, pgid=555, status="failed", exit_code=None)

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    assert caller is not None
    assert caller.record.task_id == "oooooooo"


# --- Precedence ------------------------------------------------------------------------------


def test_precedence_pb_task_id_beats_session_and_ancestry(tmp_path: Path) -> None:
    make_record(tmp_path, "by-pb-task-id", pid=100, pgid=555)
    make_record(tmp_path, "by-session", pid=200, pgid=555)
    make_record(tmp_path, "by-ancestry", pid=300, pgid=None)
    table = {999: 300}

    caller = real_detect_caller(
        tmp_path,
        environ={"PB_TASK_ID": "by-pb-task-id"},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: table,
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "pb_task_id"
    assert caller.record.task_id == "by-pb-task-id"


def test_precedence_session_beats_ancestry(tmp_path: Path) -> None:
    make_record(tmp_path, "by-session", pid=200, pgid=555)
    make_record(tmp_path, "by-ancestry", pid=300, pgid=None)
    table = {999: 300}

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: table,
        check=always("alive"),
    )

    assert caller is not None
    assert caller.method == "session"
    assert caller.record.task_id == "by-session"


# --- Nothing found -----------------------------------------------------------------------------


def test_returns_none_when_no_record_matches_anything(tmp_path: Path) -> None:
    make_record(tmp_path, "unrelated", pid=100, pgid=42)

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    assert caller is None


def test_returns_none_with_no_records_at_all(tmp_path: Path) -> None:
    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 555,
        getpid=lambda: 999,
        process_table=lambda: {},
        check=always("alive"),
    )

    assert caller is None


# --- The real process-table loader --------------------------------------------------------------


def test_load_process_table_returns_none_when_ps_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args, **kwargs):
        raise OSError("ps unavailable")

    monkeypatch.setattr(lineage.subprocess, "run", boom)

    assert lineage._load_process_table() is None


def test_detect_caller_survives_a_real_ps_failure_in_the_ancestry_step(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No injected `process_table` this time: `detect_caller` falls back to the real
    module-level loader, which is made to fail like a `ps`-less environment would."""
    monkeypatch.setattr(lineage, "_table_cache", None)
    monkeypatch.setattr(lineage, "_table_cached_at", None)

    def boom(*args, **kwargs):
        raise OSError("ps unavailable")

    monkeypatch.setattr(lineage.subprocess, "run", boom)
    make_record(tmp_path, "unreached", pid=100, pgid=None)

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=raising_getsid,
        getpid=lambda: 999,
        check=always("alive"),
    )

    assert caller is None


# --- Process-table cache TTL ---------------------------------------------------------------------


class _FakePsResult:
    def __init__(self, stdout: str, returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def test_process_table_cache_honours_its_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lineage, "_table_cache", None)
    monkeypatch.setattr(lineage, "_table_cached_at", None)

    calls: list[int] = []

    def fake_run(*args, **kwargs):
        calls.append(1)
        return _FakePsResult("100 1\n200 100\n")

    monkeypatch.setattr(lineage.subprocess, "run", fake_run)

    clock = {"t": 0.0}
    monkeypatch.setattr(lineage.time, "monotonic", lambda: clock["t"])

    first = lineage.process_table()
    assert first == {100: 1, 200: 100}
    assert len(calls) == 1

    clock["t"] = lineage.PROCESS_TABLE_TTL_SECONDS - 0.1
    second = lineage.process_table()
    assert second == first
    assert len(calls) == 1, "still within the TTL: must not re-scan"

    clock["t"] = lineage.PROCESS_TABLE_TTL_SECONDS + 0.1
    third = lineage.process_table()
    assert third == first
    assert len(calls) == 2, "past the TTL: must re-scan"


# --- max_depth_default ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, lineage.DEFAULT_MAX_DEPTH),
        ("5", 5),
        ("-1", lineage.DEFAULT_MAX_DEPTH),
        ("-100", lineage.DEFAULT_MAX_DEPTH),
        ("abc", lineage.DEFAULT_MAX_DEPTH),
        ("", lineage.DEFAULT_MAX_DEPTH),
        ("0", 0),
        ("2", 2),
    ],
)
def test_max_depth_default(raw: str | None, expected: int) -> None:
    env = {} if raw is None else {"PB_MAX_DEPTH": raw}
    assert lineage.max_depth_default(env) == expected


def test_max_depth_default_reads_the_real_environment_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PB_MAX_DEPTH", raising=False)
    assert lineage.max_depth_default() == lineage.DEFAULT_MAX_DEPTH

    monkeypatch.setenv("PB_MAX_DEPTH", "7")
    assert lineage.max_depth_default() == 7


def test_ancestry_is_not_shadowed_by_a_dead_record_that_once_held_the_same_pid(
    tmp_path: Path,
) -> None:
    """Records come back oldest first; keeping only the first per pid would let a dead run that
    once had this pid hide the live task that holds it now — dropping lineage and the caps."""
    make_record(tmp_path, "old-run", pid=100, pgid=None, start_time="old", started_at="2026-01-01T00:00:00+00:00")
    make_record(tmp_path, "new-run", pid=100, pgid=None, start_time="new")
    table = {999: 100, 100: 1}

    caller = real_detect_caller(
        tmp_path,
        environ={},
        getsid=lambda pid: 1,
        getpid=lambda: 999,
        process_table=lambda: table,
        check=lambda identity: "alive" if identity.get("start_time") == "new" else "dead",
    )

    assert caller is not None
    assert caller.record.task_id == "new-run"


# --- detect_caller_detail: the fail-closed variant (takeover's gate) ------------------------------

from polybridge.lineage import detect_caller_detail as real_detect_caller_detail  # noqa: E402


def test_detail_positively_no_caller_once_the_negative_is_checked(tmp_path: Path) -> None:
    make_record(tmp_path, "stale", pid=100, pgid=None)  # its pid is our ancestor, but it is dead

    detection = real_detect_caller_detail(
        tmp_path,
        environ={},
        getsid=lambda _: 4242,
        getpid=lambda: 999,
        process_table=lambda: {999: 100, 100: 50},
        check=always("dead"),
    )

    assert detection == lineage.Detection(None, None)


def test_detail_reports_a_confirmed_caller(tmp_path: Path) -> None:
    make_record(tmp_path, "parent", pid=100, pgid=None)

    detection = real_detect_caller_detail(
        tmp_path,
        environ={},
        getsid=lambda _: 4242,
        getpid=lambda: 999,
        process_table=lambda: {999: 100},
        check=always("alive"),
    )

    assert detection.caller is not None and detection.caller.record.task_id == "parent"


@pytest.mark.parametrize(
    ("table", "getsid", "verdict", "reason"),
    [
        (None, lambda _: 4242, "dead", "process table could not be read"),
        ({1234: 1}, lambda _: 4242, "dead", "missing from the process table"),
        ({999: 1}, raising_getsid, "dead", "session id could not be read"),
        ({999: 100}, lambda _: 4242, "undecidable", "could not be confirmed alive or gone"),
    ],
)
def test_detail_refuses_to_call_it_negative_when_it_could_not_look(
    tmp_path: Path, table, getsid, verdict: str, reason: str
) -> None:
    make_record(tmp_path, "related", pid=100, pgid=None)

    detection = real_detect_caller_detail(
        tmp_path,
        environ={},
        getsid=getsid,
        getpid=lambda: 999,
        process_table=lambda: table,
        check=always(verdict),
    )

    assert detection.caller is None
    assert detection.undecidable is not None and reason in detection.undecidable


def test_detail_turns_an_exception_into_undecidable(tmp_path: Path) -> None:
    def boom():
        raise RuntimeError("table exploded")

    detection = real_detect_caller_detail(
        tmp_path, environ={}, getsid=lambda _: 1, getpid=lambda: 2, process_table=boom,
        check=always("dead"),
    )

    assert detection.caller is None and "table exploded" in (detection.undecidable or "")
