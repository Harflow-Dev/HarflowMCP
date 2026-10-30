"""Persisted usage-limit evidence, the bounded hit query and its MCP and ctl surfaces."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp import MCPError

from harflow_mcp import catalog, ctl, limit_hits, server, state_ownership, store, takeover, wire_contracts
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CTL_FORMAT, validate_envelope

FIXTURES = Path(__file__).parents[1] / "contracts" / "v1"
NOW = 1_800_000_000.0  # 2027-01-15, between the fixed past and future reset values below
FUTURE = "2099-01-01T00:00:00Z"
PAST = "2020-01-01T00:00:00+00:00"


def diagnostic(**fields):
    return {"category": "usage_limit", "reason": "Usage limit reached", "source": "stderr", **fields}


def day(number: int, hour: int = 0) -> str:
    return f"2026-10-{number:02d}T{hour:02d}:00:00+00:00"


def task(identifier, *, backend="codex", model="gpt-x", started=day(1), finished=day(1, 1), repo="/repo", group=None, failure=None, **fields):
    return store.TaskRecord(identifier, backend, "session", repo, started, status="failed", exit_code=1, finished_at=finished,
                            model=model, group=group, failure_diagnostic=failure, **fields)


def limit_task(identifier, **fields):
    fields.setdefault("failure", diagnostic())
    return task(identifier, **fields)


@pytest.fixture
def directory(tmp_path):
    return tmp_path / "state" / "tasks"


def recent(directory, **filters):
    return limit_hits.recent(directory, filters, now=NOW)


def hit_rows(directory):
    with catalog.Catalog(directory, store.RECORD_SUFFIX, layout=resolve_layout(log_dir=directory)).connect() as db:
        return {row[0]: row[1:] for row in db.execute("SELECT id,observed_at,backend,model,grp,repo_path FROM task_limit_hits")}


# ---- evidence extraction ----------------------------------------------------------------------------

def test_usage_limit_diagnostic_becomes_a_bounded_hit_row():
    record = limit_task("a1", title="T" * 2000, group="team",
                        failure=diagnostic(http_status=429, provider_code="rate_limit_exceeded", reset_at=FUTURE,
                                           latest_result="not kept", background_tasks=[1], reason="R" * 900))
    row = limit_hits.evidence(record)
    assert row["observed_at"] == 1_790_816_400.0 and row["backend"] == "codex" and row["model"] == "gpt-x"
    assert row["grp"] == "team" and row["repo_path"] == "/repo"
    payload = row["payload"]
    assert payload["observed_at_source"] == "finished_at" and payload["reset_at"] == FUTURE
    assert payload["diagnostic"] == {"category": "usage_limit", "source": "stderr", "reason": "R" * 500,
                                     "provider_code": "rate_limit_exceeded", "http_status": 429}
    assert len(payload["title"]) == 1000


@pytest.mark.parametrize("failure", [
    None,
    {"category": "provider_error", "reason": "Provider failed", "source": "stderr"},
    {"reason": "You have hit your usage limit", "source": "stderr"},
    "usage_limit",
    ["usage_limit"],
])
def test_only_a_usage_limit_category_is_evidence(failure):
    assert limit_hits.evidence(task("a1", failure=failure)) is None


def test_observed_at_falls_back_to_started_at_and_unorderable_records_have_no_hit():
    fallback = limit_hits.evidence(limit_task("a1", finished=None))
    assert fallback["payload"]["observed_at_source"] == "started_at" and fallback["observed_at"] == 1_790_812_800.0
    assert limit_hits.evidence(limit_task("a2", finished="garbage"))["payload"]["observed_at_source"] == "started_at"
    assert limit_hits.evidence(limit_task("a3", finished="garbage", started="garbage")) is None


def test_boolean_http_status_and_invalid_reset_are_not_kept():
    row = limit_hits.evidence(limit_task("a1", failure=diagnostic(http_status=True, reset_at="soon")))
    assert "http_status" not in row["payload"]["diagnostic"] and "reset_at" not in row["payload"]


def test_overlong_group_is_displayed_but_not_matchable():
    row = limit_hits.evidence(limit_task("a1", group="g" * 200))
    assert row["grp"] is None and row["payload"]["group"] == "g" * 200


def test_write_persists_only_usage_limit_hits(directory):
    store.write(directory, limit_task("limited", model=None))
    store.write(directory, task("plain"))
    store.write(directory, task("provider", failure={"category": "provider_error", "reason": "x", "source": "stderr"}))
    assert hit_rows(directory) == {"limited": (1_790_816_400.0, "codex", None, None, "/repo")}


def test_legacy_record_without_the_field_indexes_through_bootstrap(directory):
    store.write(directory, limit_task("legacy"))
    for suffix in ("", "-shm", "-wal"):
        Path(f"{catalog.catalog_path(directory)}{suffix}").unlink(missing_ok=True)
    result = recent(directory)
    assert [hit["task_id"] for hit in result["hits"]] == ["legacy"] and result["scan"]["complete"]


# ---- query ------------------------------------------------------------------------------------------

def test_latest_hit_per_backend_and_model_follows_observed_time_not_start_order(directory):
    # The earlier-started task finished later, so it is the latest hit.
    store.write(directory, limit_task("early-start", started=day(1), finished=day(9)))
    store.write(directory, limit_task("late-start", started=day(5), finished=day(6)))
    store.write(directory, limit_task("other-model", model="gpt-y", finished=day(2)))
    store.write(directory, limit_task("default-model", model=None, finished=day(3)))
    store.write(directory, limit_task("other-backend", backend="claude", model="gpt-x", finished=day(4)))
    result = recent(directory)
    assert [(hit["backend"], hit["requested_model"], hit["task_id"]) for hit in result["hits"]] == [
        ("codex", "gpt-x", "early-start"), ("claude", "gpt-x", "other-backend"),
        ("codex", None, "default-model"), ("codex", "gpt-y", "other-model")]
    assert result["hits"][0]["observed_at"] == "2026-10-09T00:00:00+00:00"
    assert result["hits"][0]["observed_at_source"] == "finished_at"
    assert result["scan"] == {"examined": 5, "limit": 500, "complete": True}
    assert result["index_state"]["status"] == "ready"


def test_hit_item_shape(directory):
    store.write(directory, limit_task("a1", title="Review", group="team", failure=diagnostic(http_status=429, provider_code="usage_limit_reached", reset_at=FUTURE)))
    (hit,) = recent(directory)["hits"]
    assert hit == {"backend": "codex", "requested_model": "gpt-x", "task_id": "a1", "title": "Review", "group": "team",
                   "observed_at": "2026-10-01T01:00:00+00:00", "observed_at_source": "finished_at",
                   "diagnostic": {"category": "usage_limit", "source": "stderr", "reason": "Usage limit reached",
                                  "provider_code": "usage_limit_reached", "http_status": 429},
                   "reset": {"raw": FUTURE, "at": "2099-01-01T00:00:00+00:00", "state": "future"}}


def test_provider_errors_and_ordinary_failures_are_excluded(directory):
    store.write(directory, task("plain"))
    store.write(directory, task("provider", failure={"category": "provider_error", "reason": "Provider failed", "source": "stderr"}))
    result = recent(directory)
    assert result["hits"] == [] and result["scan"] == {"examined": 0, "limit": 500, "complete": True}


def test_empty_catalog_is_ready_and_complete(directory):
    insert_hits(directory, [])
    result = recent(directory)
    assert result["hits"] == [] and result["scan"]["complete"] and result["index_state"]["status"] == "ready"


@pytest.mark.parametrize("reset_at, expected", [
    (FUTURE, {"raw": FUTURE, "at": "2099-01-01T00:00:00+00:00", "state": "future"}),
    (PAST, {"raw": PAST, "at": PAST, "state": "elapsed"}),
    ("2099-01-01T05:00:00+05:00", {"raw": "2099-01-01T05:00:00+05:00", "at": "2099-01-01T00:00:00+00:00", "state": "future"}),
    (4_102_444_800, {"raw": 4_102_444_800, "at": "2100-01-01T00:00:00+00:00", "state": "future"}),
    (1_600_000_000.5, {"raw": 1_600_000_000.5, "at": "2020-09-13T12:26:40.500000+00:00", "state": "elapsed"}),
    (None, {"state": "unknown"}),
    ("soon", {"state": "unknown"}),
    ("2099-01-01T00:00:00", {"state": "unknown"}),
    ("2099-01-01 00:00:00Z", {"state": "unknown"}),
    ("x" * 200, {"state": "unknown"}),
    (True, {"state": "unknown"}),
    (float("nan"), {"state": "unknown"}),
    ([FUTURE], {"state": "unknown"}),
    (1e300, {"raw": 1e300, "state": "unknown"}),
])
def test_reset_state(reset_at, expected):
    payload = {} if reset_at is None else {"reset_at": reset_at}
    assert limit_hits.reset_state(payload, NOW) == expected


def test_reset_state_end_to_end_future_elapsed_unknown_and_invalid(directory):
    store.write(directory, limit_task("future", model="m1", failure=diagnostic(reset_at=FUTURE)))
    store.write(directory, limit_task("elapsed", model="m2", failure=diagnostic(reset_at=PAST)))
    store.write(directory, limit_task("unknown", model="m3"))
    store.write(directory, limit_task("invalid", model="m4", failure=diagnostic(reset_at="next tuesday")))
    resets = {hit["task_id"]: hit["reset"] for hit in recent(directory)["hits"]}
    assert resets["future"]["state"] == "future" and resets["elapsed"]["state"] == "elapsed"
    assert resets["unknown"] == {"state": "unknown"} and resets["invalid"] == {"state": "unknown"}


def test_reset_is_judged_at_query_time(directory):
    store.write(directory, limit_task("a1", failure=diagnostic(reset_at="2026-12-31T00:00:00Z")))
    before = limit_hits.recent(directory, now=1_790_000_000.0)["hits"][0]["reset"]
    after = limit_hits.recent(directory, now=1_800_000_000.0)["hits"][0]["reset"]
    assert (before["state"], after["state"]) == ("future", "elapsed")


def test_filters(directory):
    store.write(directory, limit_task("c1", backend="codex", model="gpt-x", repo="/repo/a", group="alpha", finished=day(2)))
    store.write(directory, limit_task("c2", backend="codex", model="gpt-y", repo="/repo/b", group="beta", finished=day(3)))
    store.write(directory, limit_task("k1", backend="claude", model="opus", repo="/repo/a", group="beta", finished=day(4)))
    store.write(directory, limit_task("k2", backend="claude", model=None, repo="/repo/c", finished=day(5)))

    def ids(**filters):
        return sorted(hit["task_id"] for hit in recent(directory, **filters)["hits"])

    assert ids() == ["c1", "c2", "k1", "k2"]
    assert ids(backend="claude") == ["k1", "k2"]
    assert ids(model="gpt-y") == ["c2"]
    assert ids(backend="codex", model="opus") == []
    assert ids(repo="/repo/a") == ["c1", "k1"]
    assert ids(group="beta") == ["c2", "k1"]
    assert ids(since="2026-10-04T00:00:00Z") == ["k1", "k2"]
    assert ids(since=day(3)) == ["c2", "k1", "k2"]
    assert ids(backend="claude", repo="/repo/a", group="beta", since=day(1)) == ["k1"]
    assert ids(group="missing") == []


def test_independent_groups_report_their_own_latest_hit(directory):
    store.write(directory, limit_task("a1", group="alpha", finished=day(2)))
    store.write(directory, limit_task("b1", group="beta", finished=day(3)))
    assert [hit["task_id"] for hit in recent(directory)["hits"]] == ["b1"]
    assert [hit["task_id"] for hit in recent(directory, group="alpha")["hits"]] == ["a1"]
    assert recent(directory, group="alpha")["hits"][0]["group"] == "alpha"


@pytest.mark.parametrize("filters, message", [
    ({"backend": "nope"}, "nope"),
    ({"group": " "}, "group"),
    ({"group": "g" * 129}, "group"),
    ({"repo": ""}, "repo"),
    ({"model": ""}, "model"),
    ({"model": 5}, "model"),
    ({"model": "m" * 201}, "model"),
    ({"since": "yesterday"}, "since"),
    ({"since": "2026-10-01T00:00:00"}, "timezone"),
])
def test_invalid_filters_are_refused(directory, filters, message):
    store.write(directory, limit_task("a1"))
    with pytest.raises(ValueError, match=message):
        recent(directory, **filters)


# ---- bounded work -----------------------------------------------------------------------------------

def insert_hits(directory, rows):
    layout = resolve_layout(log_dir=directory)
    with state_ownership.state_session(layout):
        directory.mkdir(parents=True, exist_ok=True)
        with catalog.Catalog(directory, store.RECORD_SUFFIX, layout=layout).connect() as db:
            db.executemany("INSERT INTO task_limit_hits VALUES (?,?,?,?,?,?,?)", rows)


def window_steps(directory, filters=None, limit=500):
    layout = resolve_layout(log_dir=directory)
    index = catalog.Catalog(directory, store.RECORD_SUFFIX, layout=layout)
    steps = [0]
    with state_ownership.state_session(layout), index.connect() as db:
        db.set_progress_handler(lambda: steps.__setitem__(0, steps[0] + 1) or 0, 1)
        result = index.limit_hit_window(db, limit, filters or {})
        db.set_progress_handler(None, 0)
    return result, steps[0]


def analyze_hit_indexes(directory):
    layout = resolve_layout(log_dir=directory)
    with state_ownership.state_session(layout), catalog.Catalog(directory, store.RECORD_SUFFIX, layout=layout).connect() as db:
        db.execute("ANALYZE")


def row(number, stamp, backend="codex", model="m", group=None, repo="/r"):
    return (f"id{number:05}", stamp, backend, model, group, repo, json.dumps({"task_id": f"id{number:05}", "backend": backend}))


def test_cap_examines_exactly_500_rows_and_reports_incomplete(directory):
    for number in range(520):
        store.write(directory, limit_task(f"t{number:04}", model="newer" if number >= 20 else "oldest",
                                          finished=f"2026-09-{1 + number // 24:02d}T{number % 24:02d}:00:00+00:00"))
    result = recent(directory)
    assert result["scan"] == {"examined": 500, "limit": 500, "complete": False}
    # The 20 oldest hits fall outside the window, so their model is never seen.
    assert [hit["requested_model"] for hit in result["hits"]] == ["newer"]


def test_exactly_500_hits_is_complete(directory):
    for number in range(500):
        store.write(directory, limit_task(f"t{number:04}", finished=f"2026-09-{1 + number // 24:02d}T{number % 24:02d}:00:00+00:00"))
    assert recent(directory)["scan"] == {"examined": 500, "limit": 500, "complete": True}


def storm(directory, count=600, **fields):
    for number in range(count):
        store.write(directory, limit_task(f"s{number:04}", finished=f"2026-11-{1 + number // 24:02d}T{number % 24:02d}:00:00+00:00", **fields))


def test_unfiltered_query_sees_only_the_newest_500_hits(directory):
    storm(directory)
    store.write(directory, limit_task("old-claude", backend="claude", model="opus", finished=day(1)))
    result = recent(directory)
    assert [hit["backend"] for hit in result["hits"]] == ["codex"]
    assert result["scan"] == {"examined": 500, "limit": 500, "complete": False}


def test_backend_filter_reaches_an_older_hit_behind_another_backends_storm(directory):
    storm(directory)
    store.write(directory, limit_task("old-claude", backend="claude", model="opus", finished=day(1)))
    result = recent(directory, backend="claude")
    assert [hit["task_id"] for hit in result["hits"]] == ["old-claude"]
    assert result["scan"] == {"examined": 1, "limit": 500, "complete": True}


def test_backend_and_model_filter_reaches_an_older_hit_behind_a_storm(directory):
    storm(directory)
    store.write(directory, limit_task("old-opus", backend="claude", model="opus", finished=day(1)))
    store.write(directory, limit_task("old-haiku", backend="claude", model="haiku", finished=day(2)))
    result = recent(directory, backend="claude", model="opus")
    assert [hit["task_id"] for hit in result["hits"]] == ["old-opus"]
    assert result["scan"] == {"examined": 1, "limit": 500, "complete": True}
    only_model = recent(directory, model="haiku")
    assert [hit["task_id"] for hit in only_model["hits"]] == ["old-haiku"] and only_model["scan"]["complete"]


def test_backend_window_is_still_capped_at_500(directory):
    storm(directory)
    result = recent(directory, backend="codex")
    assert result["scan"] == {"examined": 500, "limit": 500, "complete": False}


def test_repo_and_group_filter_inside_the_backend_window(directory):
    storm(directory, 5, repo="/repo/storm")
    store.write(directory, limit_task("old", backend="claude", model="opus", repo="/repo/a", group="g", finished=day(1)))
    store.write(directory, limit_task("older", backend="claude", model="sonnet", repo="/repo/b", finished=day(1)))
    assert [hit["task_id"] for hit in recent(directory, backend="claude", repo="/repo/a")["hits"]] == ["old"]
    assert [hit["task_id"] for hit in recent(directory, backend="claude", group="g")["hits"]] == ["old"]


def test_since_still_bounds_a_backend_window(directory):
    storm(directory, 3)
    store.write(directory, limit_task("old-claude", backend="claude", model="opus", finished=day(1)))
    assert recent(directory, backend="claude", since="2026-10-02T00:00:00Z")["hits"] == []
    assert len(recent(directory, backend="claude", since="2026-10-01T00:00:00Z")["hits"]) == 1


def test_key_cap_reports_incomplete(directory):
    for number in range(55):
        store.write(directory, limit_task(f"t{number:03}", model=f"model-{number:03}", finished=day(1 + number // 24, number % 24)))
    result = recent(directory)
    assert len(result["hits"]) == 50 and result["scan"]["complete"] is False and result["scan"]["examined"] == 55


def test_tied_timestamps_keep_the_window_and_work_bounded(directory):
    insert_hits(directory, [row(n, 1_790_000_000.0) for n in range(600)])
    small, small_steps = window_steps(directory, {"backend": "codex"})
    insert_hits(directory, [row(n, 1_790_000_000.0) for n in range(600, 5000)])
    large, large_steps = window_steps(directory, {"backend": "codex"})
    assert (small[1], small[2]) == (500, True) and (large[1], large[2]) == (500, True)
    assert len(small[0]) == len(large[0]) == 500
    # Ten times the rows, all tied, does not widen the examined window.
    assert large_steps <= small_steps * 1.2
    # The same holds when an indexed backend or backend+model bound selects the window.
    for filters in ({"backend": "codex"}, {"backend": "codex", "model": "m"}, {"model": "m"}):
        (rows, examined, capped), steps = window_steps(directory, filters)
        assert (len(rows), examined, capped) == (500, 500, True)
        assert steps <= small_steps * 1.2
    # The window is the newest rows in (observed_at, id) order: ties break by id, descending.
    assert [payload["task_id"] for _, _, _, payload in large[0]][:2] == ["id04999", "id04998"]


def test_query_work_scales_with_the_500_row_window_after_analyze(directory):
    insert_hits(directory, [row(n, 1_790_000_000.0 + n, repo="/kept" if n % 2 == 0 else "/other")
                            for n in range(600)])
    analyze_hit_indexes(directory)
    filters = ({}, {"since": 1_790_000_000.0}, {"repo": "/kept"})
    small = [window_steps(directory, value) for value in filters]

    insert_hits(directory, [row(n, 1_790_000_000.0 + n, repo="/kept" if n % 2 == 0 else "/other")
                            for n in range(600, 1200)])
    analyze_hit_indexes(directory)
    large = [window_steps(directory, value) for value in filters]

    for (small_result, small_steps), (large_result, large_steps), filter_value in zip(small, large, filters):
        assert small_result[1:] == (500, True)
        assert large_result[1:] == (500, True)
        assert len(small_result[0]) == len(large_result[0]) == (250 if "repo" in filter_value else 500)
        assert large_steps <= small_steps * 1.05
        ids = [payload["task_id"] for _, _, _, payload in large_result[0]]
        assert ids == sorted(ids, reverse=True)
    # Repository filtering is applied after the capped chronology window, not before it.
    assert [payload["task_id"] for _, _, _, payload in large[2][0][0]] == [
        f"id{number:05}" for number in range(1198, 698, -2)
    ]


@pytest.mark.parametrize("bound, values, index", [
    ("", (), "task_limit_hit_chronology"),
    ("WHERE observed_at >= ? ", (0.0,), "task_limit_hit_chronology (observed_at>?)"),
    ("WHERE backend = ? ", ("codex",), "task_limit_hit_backend (backend=?)"),
    ("WHERE backend = ? AND observed_at >= ? ", ("codex", 0.0), "task_limit_hit_backend (backend=? AND observed_at>?)"),
    ("WHERE backend = ? AND model = ? ", ("codex", "m"), "task_limit_hit_backend_model (backend=? AND model=?)"),
    ("WHERE model = ? ", ("m",), "task_limit_hit_model (model=?)"),
])
def test_hit_query_plan_walks_an_ordered_index_inside_the_window(directory, bound, values, index):
    insert_hits(directory, [row(n, 1_790_000_000.0) for n in range(50)])
    layout = resolve_layout(log_dir=directory)
    with state_ownership.state_session(layout), catalog.Catalog(directory, store.RECORD_SUFFIX, layout=layout).connect() as db:
        db.execute("ANALYZE")
        plan = [detail for *_, detail in db.execute(
            "EXPLAIN QUERY PLAN SELECT t.observed_at FROM (SELECT id AS wid FROM task_limit_hits " + bound + "ORDER BY observed_at DESC,id DESC LIMIT ?) CROSS JOIN task_limit_hits t ON t.id=wid WHERE t.repo_path=? ORDER BY t.observed_at DESC,t.id DESC",
            (*values, 500, "/r"))]
    assert any("USING COVERING INDEX " + index in detail for detail in plan), plan
    assert "SEARCH t USING INDEX sqlite_autoindex_task_limit_hits_1 (id=?)" in plan
    # The inner window is ordered by the index: the only sort is the outer one over the window.
    assert plan.count("USE TEMP B-TREE FOR ORDER BY") == 1


def test_since_bound_is_applied_before_the_cap(directory):
    insert_hits(directory, [row(n, 1_790_000_000.0 + n) for n in range(700)])
    (hits, examined, capped), _ = window_steps(directory, {"since": 1_790_000_300.0})
    assert (len(hits), examined, capped) == (400, 400, False)
    (hits, examined, capped), _ = window_steps(directory, {"since": 1_790_000_100.0})
    assert (len(hits), examined, capped) == (500, 500, True)


# ---- catalog lifecycle ------------------------------------------------------------------------------

def test_rewrite_without_the_diagnostic_drops_the_hit(directory):
    record = limit_task("a1")
    store.write(directory, record)
    assert set(hit_rows(directory)) == {"a1"}
    store.write(directory, replace(record, failure_diagnostic=None))
    assert hit_rows(directory) == {} and recent(directory)["hits"] == []


def test_rewrite_replaces_the_hit(directory):
    record = limit_task("a1", model="old", finished=day(2))
    store.write(directory, record)
    store.write(directory, replace(record, model="new", finished_at=day(7, 1)))
    assert hit_rows(directory) == {"a1": (1_791_334_800.0, "codex", "new", None, "/repo")}
    assert [(hit["requested_model"], hit["observed_at"]) for hit in recent(directory)["hits"]] == [("new", "2026-10-07T01:00:00+00:00")]


def test_catalog_remove_deletes_the_hit(directory):
    store.write(directory, limit_task("a1"))
    store.write(directory, limit_task("a2", model="other"))
    catalog.Catalog(directory, store.RECORD_SUFFIX, layout=resolve_layout(log_dir=directory)).remove("a1")
    assert set(hit_rows(directory)) == {"a2"}


def test_discovery_prunes_hits_of_deleted_and_orphaned_sources(directory):
    store.write(directory, limit_task("gone"))
    store.write(directory, limit_task("kept", model="other"))
    insert_hits(directory, [row(1, 1_790_000_000.0)])
    assert set(hit_rows(directory)) == {"gone", "kept", "id00001"}
    store.record_path(directory, "gone").unlink()
    result = recent(directory)
    assert set(hit_rows(directory)) == {"kept"}
    assert [hit["task_id"] for hit in result["hits"]] == ["kept"] and result["index_state"]["status"] == "ready"


def test_unreadable_record_blocks_the_index_and_marks_the_scan_incomplete(directory):
    store.write(directory, limit_task("a1"))
    store.record_path(directory, "broken").write_text("{")
    for _ in range(3):
        result = recent(directory)
    assert result["index_state"]["status"] == "blocked"
    assert [hit["task_id"] for hit in result["hits"]] == ["a1"] and result["scan"]["complete"] is False
    assert set(hit_rows(directory)) == {"a1"}


def test_opening_a_populated_stage_c_catalog_backfills_hits_after_the_version_bump(directory, monkeypatch):
    store.write(directory, limit_task("a1"))
    store.write(directory, limit_task("a2", model="other"))
    store.write(directory, task("plain"))
    path = catalog.catalog_path(directory)
    assert path.name == ".listing.v11.sqlite3"
    # Reproduce the previous engine: same file and schema version, projection version 2, no hit rows.
    with sqlite3.connect(path) as db:
        db.execute("DELETE FROM task_limit_hits")
        db.execute("INSERT OR REPLACE INTO state VALUES ('task_projection_version','2')")
        assert db.execute("SELECT COUNT(*) FROM entries").fetchone() == (3,)
    assert catalog.TASK_PROJECTION_VERSION == "3"
    for _ in range(3):
        result = recent(directory)
    assert {hit["task_id"] for hit in result["hits"]} == {"a1", "a2"}
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT value FROM state WHERE key='task_projection_version'").fetchone() == ("3",)
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ("11",)


def test_a_catalog_that_predates_the_table_gains_it(directory):
    store.write(directory, limit_task("a1"))
    path = catalog.catalog_path(directory)
    with sqlite3.connect(path) as db:
        db.execute("DROP TABLE task_limit_hits")
        db.execute("INSERT OR REPLACE INTO state VALUES ('task_projection_version','2')")
    for _ in range(3):
        result = recent(directory)
    assert [hit["task_id"] for hit in result["hits"]] == ["a1"]


def scenario_preparing(directory):
    """101 retained records, the first and last carrying hits; a cold catalog indexes 100 per read."""
    layout = resolve_layout(log_dir=directory)
    with state_ownership.state_session(layout):
        directory.mkdir(parents=True, exist_ok=True)
        for number in range(101):
            failure = diagnostic(reset_at=FUTURE) if number in (0, 100) else None
            store.write(directory, task(f"t{number:04}", model="gpt-x" if number == 0 else "gpt-y", failure=failure,
                                        started=f"2026-10-01T00:{number // 60:02d}:{number % 60:02d}+00:00",
                                        finished=f"2026-10-01T01:{number // 60:02d}:{number % 60:02d}+00:00"))
        for suffix in ("", "-shm", "-wal"):
            Path(f"{catalog.catalog_path(directory)}{suffix}").unlink(missing_ok=True)


def test_preparing_catalog_returns_indexed_hits_flagged_incomplete(directory):
    scenario_preparing(directory)
    first = recent(directory)
    assert first["index_state"]["status"] == "preparing" and first["index_state"]["pending_records"] == 1
    assert [hit["task_id"] for hit in first["hits"]] == ["t0000"] and first["scan"]["complete"] is False
    second = recent(directory)
    assert second["index_state"]["status"] == "ready" and second["scan"]["complete"] is True
    assert {hit["task_id"] for hit in second["hits"]} == {"t0000", "t0100"}


# ---- MCP --------------------------------------------------------------------------------------------

@pytest.fixture
def mcp_state(directory):
    # The autouse registry fixture already points at this directory.
    assert server._reg().log_dir == directory
    return directory


async def test_mcp_tool_returns_hits_for_a_direct_caller(mcp_state):
    store.write(mcp_state, limit_task("a1", failure=diagnostic(reset_at=FUTURE)))
    store.write(mcp_state, limit_task("a2", backend="claude", model=None, finished=day(3)))
    result = await server.recent_limit_hits()
    assert {hit["task_id"] for hit in result["hits"]} == {"a1", "a2"}
    assert [hit["task_id"] for hit in (await server.recent_limit_hits(backend="claude", group=None, repo="/repo", model=None))["hits"]] == ["a2"]
    assert (await server.recent_limit_hits(since="2026-10-02T00:00:00Z"))["hits"][0]["task_id"] == "a2"


async def test_mcp_tool_rejects_bad_filters_as_invalid_params(mcp_state):
    store.write(mcp_state, limit_task("a1"))
    for filters in ({"backend": "nope"}, {"since": "yesterday"}, {"group": " "}):
        with pytest.raises(MCPError) as caught:
            await server.recent_limit_hits(**filters)
        assert caught.value.error.code == -32602


async def test_managed_and_unverifiable_callers_are_refused_before_any_lookup(mcp_state, monkeypatch):
    store.write(mcp_state, limit_task("a1"))
    monkeypatch.setattr(limit_hits, "recent", lambda *a, **k: pytest.fail("lookup before refusal"))
    monkeypatch.setattr(catalog.Catalog, "connect", lambda *a, **k: pytest.fail("catalog opened before refusal"))

    async def managed():
        return SimpleNamespace(record=SimpleNamespace(task_id="worker"))

    monkeypatch.setattr(server, "_verified_workflow_caller", managed)
    with pytest.raises(MCPError, match="direct caller") as caught:
        await server.recent_limit_hits()
    assert caught.value.error.code == -32602

    async def undecidable():
        raise MCPError(-32602, "Workflow caller authority is undecidable: ps unavailable")

    monkeypatch.setattr(server, "_verified_workflow_caller", undecidable)
    with pytest.raises(MCPError, match="undecidable"):
        await server.recent_limit_hits()


async def test_mcp_tool_is_registered_with_documented_arguments():
    from mcp import Client
    async with Client(server.mcp) as client:
        tools = {tool.name: tool for tool in (await client.list_tools()).tools}
    tool = tools["recent_limit_hits"]
    assert set(tool.input_schema["properties"]) == {"backend", "model", "since", "repo", "group"}
    description = " ".join(tool.description.split())
    assert "direct callers only" in description and "does not say the provider is available" in description


# ---- ctl and contract fixtures ----------------------------------------------------------------------

@pytest.fixture
def ctl_state(directory, monkeypatch):
    monkeypatch.setattr(ctl, "default_log_dir", lambda: directory)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *a, **k: None)
    return directory


def run_ctl(capsys, *arguments):
    code = ctl.main(["limit-hits", *arguments, "--json"])
    return code, json.loads(capsys.readouterr().out)


def scenario_ready(directory):
    store.write(directory, limit_task("fixture-codex-new", title="Review parser", group="fixture-group", started="2026-10-27T00:00:00+00:00", finished="2026-10-27T01:00:00+00:00",
                                      failure=diagnostic(http_status=429, provider_code="usage_limit_reached", reset_at=FUTURE)))
    store.write(directory, limit_task("fixture-codex-old", model="gpt-y", started="2026-10-26T00:00:00+00:00", finished="2026-10-26T01:00:00+00:00",
                                      failure=diagnostic(reset_at=PAST)))
    store.write(directory, limit_task("fixture-codex-default", model=None, started="2026-10-25T00:00:00+00:00", finished="2026-10-25T01:00:00+00:00",
                                      failure=diagnostic(reset_at=1_600_000_000)))
    store.write(directory, limit_task("fixture-claude", backend="claude", model="opus", repo="/fixture/other", started="2026-10-24T00:00:00+00:00", finished=None,
                                      failure=diagnostic(source="result", reset_at="not a date")))
    store.write(directory, task("fixture-provider-error", failure={"category": "provider_error", "reason": "Provider failed", "source": "stderr"}))


def scenario_empty(directory):
    store.write(directory, task("fixture-plain", started="2026-10-27T00:00:00+00:00", finished="2026-10-27T01:00:00+00:00"))


FIXTURE_SCENARIOS = {
    "ctl-limit-hits-ready.json": scenario_ready,
    "ctl-limit-hits-empty.json": scenario_empty,
    "ctl-limit-hits-preparing.json": scenario_preparing,
}


@pytest.mark.parametrize("name", sorted(FIXTURE_SCENARIOS))
def test_ctl_output_matches_the_canonical_fixture(name, ctl_state, capsys):
    FIXTURE_SCENARIOS[name](ctl_state)
    code, document = run_ctl(capsys)
    assert code == 0
    assert document == json.loads((FIXTURES / name).read_text())


@pytest.mark.parametrize("name", sorted(FIXTURE_SCENARIOS))
def test_limit_hit_fixtures_are_manifested_and_wire_valid(name):
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    case = manifest["cases"][name]
    assert case == {"encoding": "json", "expected_product_version": wire_contracts.PRODUCT_VERSION, "format": CTL_FORMAT,
                    "required_capabilities": [], "valid": True}
    assert manifest["files"][name] == hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest()
    document = json.loads((FIXTURES / name).read_text())
    validate_envelope(document, CTL_FORMAT)
    assert set(document["result"]) == {"hits", "scan", "index_state"}


def test_fixtures_cover_each_reset_state_and_the_incomplete_flags():
    ready = json.loads((FIXTURES / "ctl-limit-hits-ready.json").read_text())["result"]
    assert {hit["reset"]["state"] for hit in ready["hits"]} == {"future", "elapsed", "unknown"}
    assert any("raw" not in hit["reset"] for hit in ready["hits"]) and any(hit["observed_at_source"] == "started_at" for hit in ready["hits"])
    assert ready["scan"]["complete"] is True
    empty = json.loads((FIXTURES / "ctl-limit-hits-empty.json").read_text())["result"]
    assert empty["hits"] == [] and empty["scan"]["complete"] is True
    preparing = json.loads((FIXTURES / "ctl-limit-hits-preparing.json").read_text())["result"]
    assert preparing["scan"]["complete"] is False and preparing["index_state"]["status"] == "preparing"


def test_ctl_filters_and_text_output(ctl_state, capsys):
    scenario_ready(ctl_state)
    code, document = run_ctl(capsys, "--backend", "claude", "--repo", "/fixture/other", "--model", "opus", "--since", "2026-10-01T00:00:00Z")
    assert code == 0 and [hit["task_id"] for hit in document["result"]["hits"]] == ["fixture-claude"]
    code, document = run_ctl(capsys, "--group", "fixture-group")
    assert [hit["task_id"] for hit in document["result"]["hits"]] == ["fixture-codex-new"]
    assert ctl.main(["limit-hits"]) == 0
    assert json.loads(capsys.readouterr().out)["scan"]["limit"] == 500


def test_ctl_invalid_filter_is_invalid_params(ctl_state, capsys):
    scenario_empty(ctl_state)
    code, document = run_ctl(capsys, "--since", "yesterday")
    assert code == 1 and document["error"]["code"] == "invalid_params" and "since" in document["error"]["message"]


def test_ctl_reports_an_unreadable_catalog_as_limit_hits_error(ctl_state, capsys, monkeypatch):
    scenario_empty(ctl_state)

    def unavailable(*args, **kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(limit_hits, "recent", unavailable)
    code, document = run_ctl(capsys)
    assert code == 1 and document["error"]["code"] == "limit_hits_error" and "disk unavailable" in document["error"]["message"]
    monkeypatch.setattr(limit_hits, "recent", lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError("locked")))
    assert run_ctl(capsys)[1]["error"]["code"] == "limit_hits_error"


@pytest.mark.parametrize("code", ["agent_caller", "caller_undecidable"])
def test_ctl_refuses_agent_callers_before_any_lookup(code, ctl_state, capsys, monkeypatch):
    scenario_empty(ctl_state)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *a, **k: (code, "called from task worker"))
    monkeypatch.setattr(limit_hits, "recent", lambda *a, **k: pytest.fail("lookup before refusal"))
    exit_code, document = run_ctl(capsys)
    assert exit_code == 1 and document["error"]["code"] == code and "direct caller" in document["error"]["message"]


def test_ctl_command_is_read_only_and_needs_no_mutation_authority(ctl_state, capsys, monkeypatch):
    from harflow_mcp import caller_policy
    scenario_empty(ctl_state)
    monkeypatch.setattr(caller_policy, "require_mutation", lambda *a, **k: pytest.fail("limit-hits is read only"))
    assert run_ctl(capsys)[0] == 0


def test_capability_is_advertised_by_the_engine_and_the_canonical_handshake_fixture():
    assert "limit-hits.v1" in wire_contracts.CAPABILITIES
    assert "limit-hits.v1" in wire_contracts.version_info()["capabilities"]
    document = json.loads((FIXTURES / "version-info.json").read_text())
    assert document["capabilities"] == list(wire_contracts.CAPABILITIES)
