"""Bounded run listing filters, scan window and field projections."""
from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import pytest
from mcp import MCPError

from harflow_mcp import catalog, ctl, lineage, server, state_ownership, store, workflow_reads, workflow_responses, workflows
from harflow_mcp.state_layout import resolve_layout

BASE = 1_700_000_000.0
RUN = {"id": "run_id", "ts": "created_at"}


def _definition(name="Filter Fixture", workflow_id="workflow-generation-1"):
    value = {
        "name": name, "routing_mode": "explicit", "orchestrator": {"backend": "codex"},
        "nodes": [{"id": "start", "type": "start"},
                  {"id": "work", "type": "agent", "role": "task", "instructions": "Fixture", "agent": {"backend": "codex"}},
                  {"id": "end", "type": "end"}],
        "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]}
    if workflow_id is not None:
        value["workflow_id"] = workflow_id
    return workflows.validate_definition(value)


class Clock:
    def __init__(self, value=BASE):
        self.value = value

    def time(self):
        return self.value


@pytest.fixture
def owned_state(tmp_path):
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        yield root


@pytest.fixture
def clock(monkeypatch):
    value = Clock()
    monkeypatch.setattr(workflows, "time", value)
    return value


@pytest.fixture
def storage(owned_state, tmp_path, clock, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda _backend: True)
    return workflows.WorkflowStore(root=owned_state)


@pytest.fixture
def repos(tmp_path):
    result = {}
    for name in ("alpha", "beta"):
        result[name] = tmp_path / name
        result[name].mkdir()
    return result


def make(storage, clock, repo, *, name="Filter Fixture", workflow_id="workflow-generation-1", status=None, created=BASE, parent=None, **extra):
    clock.value = created
    run = storage.create_run(_definition(name, workflow_id), "prompt", repo)

    def mutate(value):
        if status is not None:
            value["status"] = status
        if parent is not None:
            value["parent_link"] = {"workflow_run_id": parent, "root_workflow_run_id": parent, "execution_id": "e1", "node_id": "work", "depth": 1}
        value.update(extra)

    if status is not None or parent is not None or extra:
        storage.update_run(run["workflow_run_id"], mutate, "fixture")
    return run["workflow_run_id"]


def ids(page):
    return [item["workflow_run_id"] for item in page["items"]]


def walk(storage, **kwargs):
    """Follow cursors to the end; returns (all ids, pages)."""
    pages, found, cursor = [], [], None
    while True:
        page = storage.list_run_page(cursor=cursor, **kwargs)
        pages.append(page)
        found.extend(ids(page))
        cursor = page["next_cursor"]
        if not page["has_more"]:
            return found, pages
        assert cursor is not None
        assert len(pages) < 500


@pytest.fixture
def mixed(storage, clock, repos):
    """Eight runs, newest first: a1 b1 ... with distinct attributes."""
    rows = {}
    rows["root_done"] = make(storage, clock, repos["alpha"], name="Nightly Build", workflow_id="wf-nightly", status="completed", created=BASE + 100)
    rows["root_failed"] = make(storage, clock, repos["beta"], name="Release STRASSE", workflow_id="wf-release", status="failed", created=BASE + 90)
    rows["child_done"] = make(storage, clock, repos["alpha"], name="Nightly Build", workflow_id="wf-nightly", status="completed", created=BASE + 80, parent=rows["root_done"])
    rows["root_run"] = make(storage, clock, repos["alpha"], name="Docs Sweep", workflow_id="wf-docs", status="running", created=BASE + 70)
    rows["child_failed"] = make(storage, clock, repos["beta"], name="Release STRASSE", workflow_id="wf-release", status="failed", created=BASE + 60, parent=rows["root_failed"])
    rows["root_paused"] = make(storage, clock, repos["beta"], name="Docs Sweep", workflow_id="wf-docs", status="paused", created=BASE + 50)
    rows["legacy"] = make(storage, clock, repos["alpha"], name="Legacy Thing", workflow_id=None, status="cancelled", created=BASE + 40)
    return rows


def iso(seconds):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def names(storage, **filters):
    return set(ids(storage.list_run_page(filters=filters)))


def test_unfiltered_page_is_unchanged(storage, mixed):
    page = storage.list_run_page()
    assert len(page["items"]) == 7 and "scan_incomplete" not in page
    assert page["total_active_count"] == 2
    # Only unset filters (and scope all) leave the unfiltered path.
    assert "scan_incomplete" not in storage.list_run_page(filters={"scope": "all", "repo": None})


def test_each_filter_alone(storage, mixed, repos):
    r = mixed
    assert names(storage, workflow_id="wf-nightly") == {r["root_done"], r["child_done"]}
    assert names(storage, repo=str(repos["beta"].resolve())) == {r["root_failed"], r["child_failed"], r["root_paused"]}
    assert names(storage, status=["failed"]) == {r["root_failed"], r["child_failed"]}
    assert names(storage, status=["failed", "running", "paused"]) == {r["root_failed"], r["child_failed"], r["root_run"], r["root_paused"]}
    assert names(storage, since=iso(BASE + 80)) == {r["root_done"], r["root_failed"], r["child_done"]}
    assert names(storage, before=iso(BASE + 60)) == {r["root_paused"], r["legacy"], r["child_failed"]} - {r["child_failed"]}
    assert names(storage, title_contains="docs") == {r["root_run"], r["root_paused"]}
    assert names(storage, scope="root") == {r["root_done"], r["root_failed"], r["root_run"], r["root_paused"], r["legacy"]}
    assert names(storage, scope="nested") == {r["child_done"], r["child_failed"]}
    # Legacy definitions without a stable id use their derived identity.
    legacy_id = storage.list_run_page(run_ids=[r["legacy"]])["items"][0]["workflow_id"]
    assert legacy_id.startswith("legacy-")
    assert names(storage, workflow_id=legacy_id) == {r["legacy"]}


def test_filters_combine_with_and(storage, mixed, repos):
    r = mixed
    assert names(storage, workflow_id="wf-release", scope="nested") == {r["child_failed"]}
    assert names(storage, repo=str(repos["alpha"].resolve()), status=["completed"], scope="root") == {r["root_done"]}
    assert names(storage, workflow_id="wf-docs", status=["paused"], since=iso(BASE + 45), before=iso(BASE + 55), title_contains="SWEEP") == {r["root_paused"]}
    assert names(storage, workflow_id="wf-docs", status=["failed"]) == set()


def test_since_is_inclusive_and_before_exclusive(storage, mixed):
    r = mixed
    assert names(storage, since=iso(BASE + 90), before=iso(BASE + 100)) == {r["root_failed"]}
    assert names(storage, since=iso(BASE + 90), before=iso(BASE + 90)) == set()
    assert names(storage, since=iso(BASE + 100)) == {r["root_done"]}
    assert names(storage, before=iso(BASE + 40)) == set()
    assert names(storage, before=iso(BASE + 40.5)) == {r["legacy"]}
    # Offsets are honored, not read as UTC.
    assert names(storage, since="2023-11-14T22:14:20+00:00") == names(storage, since="2023-11-15T00:14:20+02:00")


def test_title_contains_is_casefolded_both_ways(storage, mixed):
    r = mixed
    for text in ("release strasse", "RELEASE STRASSE", "StRaSsE", "release straße"):
        assert names(storage, title_contains=text) == {r["root_failed"], r["child_failed"]}
    assert names(storage, title_contains="nightly bui") == {r["root_done"], r["child_done"]}
    assert names(storage, title_contains="absent") == set()


def test_scope_values(storage, mixed):
    assert names(storage, scope="all") == names(storage) == set(mixed.values())
    assert len(names(storage, scope="root") | names(storage, scope="nested")) == 7
    assert names(storage, scope="root") & names(storage, scope="nested") == set()


def test_filtered_reply_shape(storage, mixed):
    page = storage.list_run_page(filters={"status": ["completed"]})
    assert page["scan_incomplete"] is False
    assert page["total_active_count"] is None
    assert page["has_more"] is False and page["next_cursor"] is None
    # Root counts are unfiltered and still computed.
    assert page["total_active_root_count"] == 2 and page["counts_complete"]


def test_active_only_combines_with_filters(storage, mixed):
    r = mixed
    assert ids(storage.list_run_page(active_only=True, filters={"scope": "root"})) == [r["root_run"], r["root_paused"]]
    assert set(ids(storage.list_run_page(active_only=True, filters={"workflow_id": "wf-docs"}))) == {r["root_run"], r["root_paused"]}
    assert ids(storage.list_run_page(active_only=True, filters={"status": ["completed"]})) == []


@pytest.mark.parametrize("filters,message", [
    ({"nope": "x"}, "unknown run listing filters"),
    ({"repoo": "x"}, "did you mean 'repo'"),
    ({"status": ["done"]}, "unknown run statuses"),
    ({"status": ["complete"]}, "did you mean 'completed'"),
    ({"status": []}, "nonempty list"),
    ({"status": [1]}, "nonempty list"),
    ({"scope": "everything"}, "scope must be one of"),
    ({"since": "2026-01-01T00:00:00"}, "timezone"),
    ({"before": "yesterday"}, "ISO8601"),
    ({"workflow_id": ""}, "nonblank string"),
    ({"title_contains": "  "}, "nonblank string"),
    ({"repo": "x" * 5000}, "at most 4096"),
    ({"title_contains": "é" * 101}, "at most 200"),
])
def test_invalid_filters_are_refused(storage, filters, message):
    with pytest.raises(ValueError, match=message):
        storage.list_run_page(filters=filters)


def test_validator_normalizes():
    assert workflow_reads.validate_run_filters({"status": ["failed", "completed", "failed"], "title_contains": "ABC", "scope": "all", "repo": None}) == {"status": ["completed", "failed"], "title_contains": "abc"}
    assert workflow_reads.validate_run_filters({"status": "failed"}) == {"status": ["failed"]}
    assert workflow_reads.validate_run_filters({}) == {}


# ---- scan window ----------------------------------------------------------------------------

def test_scan_incomplete_resumes_from_the_last_examined_row(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 7)
    created = {}
    for number in range(1, 21):
        created[number] = make(storage, clock, repos["alpha"], workflow_id="wf-hit" if number in {13, 2} else "wf-miss", created=BASE + number)
    filters = {"workflow_id": "wf-hit"}
    first = storage.list_run_page(filters=filters)
    assert first["items"] == [] and first["scan_incomplete"] is True and first["has_more"] is True
    second = storage.list_run_page(filters=filters, cursor=first["next_cursor"])
    assert ids(second) == [created[13]] and second["scan_incomplete"] is True and second["has_more"] is True
    third = storage.list_run_page(filters=filters, cursor=second["next_cursor"])
    assert ids(third) == [created[2]] and third["scan_incomplete"] is False and third["has_more"] is False and third["next_cursor"] is None
    # The first cursor sits on the last examined (7th newest) row.
    decoded = json.loads(__import__("base64").urlsafe_b64decode(first["next_cursor"]))
    assert (decoded["id"], decoded["stamp"]) == (created[14], BASE + 14)
    found, pages = walk(storage, filters=filters)
    assert found == [created[13], created[2]] and len(pages) == 3


def test_a_window_that_ends_with_the_catalog_is_complete(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 7)
    for number in range(7):
        make(storage, clock, repos["alpha"], created=BASE + number)
    page = storage.list_run_page(filters={"workflow_id": "wf-none"})
    assert page["scan_incomplete"] is False and page["has_more"] is False and page["next_cursor"] is None
    make(storage, clock, repos["alpha"], created=BASE + 100)
    page = storage.list_run_page(filters={"workflow_id": "wf-none"})
    assert page["scan_incomplete"] is True and page["has_more"] is True


def test_a_full_page_inside_the_window_is_not_scan_incomplete(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 7)
    created = [make(storage, clock, repos["alpha"], created=BASE + number) for number in range(30)]
    page = storage.list_run_page(limit=2, filters={"workflow_id": "workflow-generation-1"})
    assert ids(page) == created[::-1][:2] and page["scan_incomplete"] is False and page["has_more"] is True
    again = storage.list_run_page(limit=2, filters={"workflow_id": "workflow-generation-1"}, cursor=page["next_cursor"])
    assert ids(again) == created[::-1][2:4]


def test_window_respects_active_only_scope(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 3)
    done = [make(storage, clock, repos["alpha"], status="completed", created=BASE + n) for n in range(10)]
    live = [make(storage, clock, repos["alpha"], status="running", created=BASE + 100 + n, workflow_id="wf-live") for n in range(2)]
    # Only active rows are examined, so the window is not spent on the completed history.
    page = storage.list_run_page(active_only=True, filters={"workflow_id": "wf-live"})
    assert sorted(ids(page)) == sorted(live) and page["scan_incomplete"] is False
    # Without the scope the three newest rows are the whole window, and older history remains.
    page = storage.list_run_page(filters={"workflow_id": "wf-live"})
    assert sorted(ids(page)) == sorted(live) and page["scan_incomplete"] is True and page["has_more"] is True
    assert len(done) == 10


def test_three_hundred_runs_page_across_windows(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 100)
    expected = {"wf-a": [], "wf-b": [], "wf-c": []}
    statuses = ["completed", "failed", "running"]
    roots = {}
    for number in range(320):
        workflow = ("wf-a", "wf-b", "wf-c")[number % 3] if number % 7 else "wf-c"
        status = statuses[number % 3]
        parent = roots.get(workflow) if number % 5 == 0 and roots.get(workflow) else None
        run = make(storage, clock, repos["alpha" if number % 2 else "beta"], workflow_id=workflow, status=status, created=BASE + number, parent=parent)
        roots.setdefault(workflow, run)
        expected[workflow].append((run, status, parent is None, number % 2))
    def check(filters, predicate, limit):
        found, pages = walk(storage, limit=limit, filters=filters)
        want = [row[0] for rows in expected.values() for row in rows if predicate(row)]
        assert sorted(found) == sorted(want) and len(found) == len(set(found))
        stamps = [storage.list_run_page(run_ids=[run])["items"][0]["created_at"] for run in found]
        assert stamps == sorted(stamps, reverse=True)
        for page in pages:
            assert len(json.dumps(page).encode()) < catalog.PAGE_BYTES
            assert page["total_active_count"] is None
        return pages
    pages = check({"workflow_id": "wf-b"}, lambda row: row[0] in {r[0] for r in expected["wf-b"]}, 100)
    assert any(page["scan_incomplete"] for page in pages)
    check({"status": ["failed"], "scope": "root"}, lambda row: row[1] == "failed" and row[2], 10)
    check({"scope": "nested"}, lambda row: not row[2], 7)
    check({"workflow_id": "wf-c", "repo": str(repos["beta"].resolve())}, lambda row: row[0] in {r[0] for r in expected["wf-c"]} and not row[3], 100)


def test_concurrent_changes_between_pages_keep_paging_stable(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 6)
    runs = [make(storage, clock, repos["alpha"], status="completed", created=BASE + n) for n in range(24)]
    newest_first = runs[::-1]
    first = storage.list_run_page(limit=3, filters={"status": ["completed"]})
    assert ids(first) == newest_first[:3]
    # A newer run appears, an older match stops matching, and another run is rewritten in place.
    fresh = make(storage, clock, repos["alpha"], status="completed", created=BASE + 500)
    storage.update_run(newest_first[5], lambda value: value.update(status="failed"), "changed")
    storage.update_run(newest_first[7], lambda value: value.update(summary="rewritten"), "rewritten")
    found, cursor = list(ids(first)), first["next_cursor"]
    while cursor:
        page = storage.list_run_page(limit=3, filters={"status": ["completed"]}, cursor=cursor)
        found.extend(ids(page))
        cursor = page["next_cursor"] if page["has_more"] else None
    assert fresh not in found
    assert found == [run for run in newest_first if run != newest_first[5]]


# ---- cursors and restrictions ---------------------------------------------------------------

def test_cursor_is_bound_to_its_filters(storage, clock, repos):
    for number in range(6):
        make(storage, clock, repos["alpha"], status="completed", created=BASE + number)
    first = storage.list_run_page(limit=2, filters={"status": ["completed"]})
    assert first["next_cursor"]
    for other in ({"status": ["failed"]}, {"status": ["completed"], "scope": "root"}, {"workflow_id": "workflow-generation-1"}, {}):
        with pytest.raises(ValueError, match="Invalid listing cursor"):
            storage.list_run_page(limit=2, filters=other, cursor=first["next_cursor"])
    # Equivalent spellings of the same filters share a scope.
    again = storage.list_run_page(limit=2, filters={"scope": "all", "status": ["completed", "completed"]}, cursor=first["next_cursor"])
    assert len(again["items"]) == 2
    unfiltered = storage.list_run_page(limit=2)
    with pytest.raises(ValueError, match="Invalid listing cursor"):
        storage.list_run_page(limit=2, filters={"status": ["completed"]}, cursor=unfiltered["next_cursor"])


async def test_filters_conflict_with_run_ids_and_related_run_id(owned_state):
    directory = owned_state / "tasks"
    with pytest.raises(ValueError, match="cannot be combined"):
        await workflow_reads.call("list_run_page", directory=directory, limit=10, run_ids=["x"], filters={"scope": "root"})
    with pytest.raises(ValueError, match="cannot be combined"):
        await workflow_reads.call("list_run_page", directory=directory, limit=10, related_run_id="x", filters={"status": ["failed"]})
    with pytest.raises(ValueError, match="unknown run listing filters"):
        await workflow_reads.call("list_run_page", directory=directory, limit=10, filters={"group": "g"})
    # Unset filters do not conflict.
    page = await workflow_reads.call("list_run_page", directory=directory, limit=10, run_ids=[], filters={"scope": None, "repo": None})
    assert page["items"] == []


async def test_server_tool_passes_filters_and_fields(monkeypatch):
    seen = {}

    async def fake(action, **kwargs):
        seen.update(action=action, **kwargs)
        return {"items": []}

    monkeypatch.setattr(server, "_workflow_call", fake)
    await server.list_workflow_run_page(workflow_id="w", repo="/r", status=["failed"], since="2026-01-01T00:00:00Z", before="2026-02-01T00:00:00Z", title_contains="T", scope="nested", fields=["name"])
    assert seen["action"] == "list_run_page" and seen["fields"] == ["name"]
    assert seen["filters"] == {"workflow_id": "w", "repo": "/r", "status": ["failed"], "since": "2026-01-01T00:00:00Z", "before": "2026-02-01T00:00:00Z", "title_contains": "T", "scope": "nested"}


# ---- catalog (run_meta) ----------------------------------------------------------------------

def run_meta_rows(storage):
    with catalog.Catalog(storage.runs, ".json", layout=storage.state_layout).connect() as db:
        return {row[0]: row[1:] for row in db.execute("SELECT id,workflow_id,repo_path,name_folded,is_root FROM run_meta")}


def test_run_meta_is_lossless_beyond_header_truncation(storage, clock, repos):
    deep = repos["alpha"].joinpath(*["segment-" + "d" * 30] * 8)
    deep.mkdir(parents=True)
    long_name = "Long Name " + "X" * 90
    run = make(storage, clock, deep, name=long_name, workflow_id="wf-" + "w" * 150, status="completed")
    header = storage.list_run_page(run_ids=[run])["items"][0]
    assert len(header["repo_path"]) < len(str(deep.resolve())) > 250
    row = run_meta_rows(storage)[run]
    assert row == ("wf-" + "w" * 150, str(deep.resolve()), long_name.casefold(), 1)
    assert names(storage, repo=str(deep.resolve())) == {run}
    assert names(storage, title_contains="x" * 90) == {run}
    assert names(storage, workflow_id="wf-" + "w" * 150) == {run}


def test_schema_is_v11_and_old_catalogs_are_left_alone(storage, clock, repos):
    run = make(storage, clock, repos["alpha"])
    old = storage.runs / ".listing.v10.sqlite3"
    old.write_bytes(b"not a database; an older engine owns this file")
    assert catalog.catalog_path(storage.runs).name == ".listing.v11.sqlite3"
    assert names(storage, scope="root") == {run}
    assert old.read_bytes() == b"not a database; an older engine owns this file"
    with sqlite3.connect(catalog.catalog_path(storage.runs)) as db:
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ("11",)
        assert db.execute("SELECT name FROM sqlite_master WHERE name='run_meta'").fetchone() == ("run_meta",)


def test_catalog_with_an_older_schema_version_is_rebuilt(storage, clock, repos):
    kept = make(storage, clock, repos["alpha"], workflow_id="wf-kept", created=BASE + 1)
    other = make(storage, clock, repos["beta"], workflow_id="wf-other", created=BASE + 2)
    assert names(storage, workflow_id="wf-kept") == {kept}
    path = catalog.catalog_path(storage.runs)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE state SET value='10' WHERE key='schema_version'")
        db.execute("DELETE FROM run_meta")
    # Opening resets the derivative; bounded discovery repopulates entries and run_meta.
    first = storage.list_run_page(filters={"workflow_id": "wf-kept"})
    found, _ = walk(storage, filters={"workflow_id": "wf-kept"})
    assert found == [kept]
    assert set(run_meta_rows(storage)) == {kept, other}
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ("11",)
    assert first is not None


def test_deleted_catalog_is_rebuilt_with_run_meta(storage, clock, repos):
    a = make(storage, clock, repos["alpha"], workflow_id="wf-a", created=BASE + 1)
    make(storage, clock, repos["beta"], workflow_id="wf-b", created=BASE + 2)
    catalog.catalog_path(storage.runs).unlink()
    found = []
    for _ in range(20):
        page = storage.list_run_page(filters={"workflow_id": "wf-a"})
        if not page["bootstrap_pending"]:
            found = ids(page)
            break
    assert found == [a]


def test_run_meta_follows_updates_and_removal(storage, clock, repos):
    run = make(storage, clock, repos["alpha"], workflow_id="wf-a", status="running")
    gone = make(storage, clock, repos["beta"], workflow_id="wf-b", created=BASE + 1)
    storage.list_run_page()
    assert set(run_meta_rows(storage)) == {run, gone}
    # An update re-indexes the same row.
    storage.update_run(run, lambda value: value.update(name="Renamed Run"), "rename")
    assert run_meta_rows(storage)[run][2] == "renamed run"
    assert names(storage, title_contains="renamed") == {run}
    # Deleting the source drops the row at the next discovery.
    (storage.runs / f"{gone}.json").unlink()
    storage.list_run_page()
    assert set(run_meta_rows(storage)) == {run}
    assert names(storage, workflow_id="wf-b") == set()


def test_catalog_remove_and_discovery_prune_run_meta(storage, clock, repos):
    run = make(storage, clock, repos["alpha"], workflow_id="wf-a")
    other = make(storage, clock, repos["alpha"], workflow_id="wf-a", created=BASE + 1)
    storage.list_run_page()
    index = catalog.Catalog(storage.runs, ".json", layout=storage.state_layout)
    (storage.runs / f"{run}.json").unlink()
    index.remove(run)
    assert set(run_meta_rows(storage)) == {other}
    # A row with no source or entry is an orphan that the stale union reconciles.
    with index.connect() as db:
        db.execute("INSERT INTO run_meta VALUES ('orphan','wf-a','/r','x',1)")
        db.execute("DELETE FROM state WHERE key='complete'")
    storage.list_run_page()
    assert set(run_meta_rows(storage)) == {other}


def test_unreadable_runs_have_no_run_meta_and_never_match_filters(storage, clock, repos):
    good = make(storage, clock, repos["alpha"], workflow_id="wf-a")
    (storage.runs / "broken.json").write_text("{not json")
    page = storage.list_run_page(filters={"workflow_id": "wf-a"})
    for _ in range(5):
        if not page["bootstrap_pending"]:
            break
        page = storage.list_run_page(filters={"workflow_id": "wf-a"})
    assert ids(page) == [good]
    assert page["history_incomplete"] is True
    assert "broken" not in run_meta_rows(storage)


# ---- managed callers ------------------------------------------------------------------------

@pytest.fixture
def managed_run(monkeypatch, tmp_path):
    from harflow_mcp import workflows as workflow_module
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(workflow_module.backends, "is_installed", lambda _backend: True)
    storage = workflow_module.WorkflowStore()
    definition = storage.save("owned-graph", {
        "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
        "connections": [{"id": "finish", "source": "start", "target": "end"}]})
    repo = tmp_path / "repo"
    repo.mkdir()

    def build(role):
        run = storage.create_run(definition, "synthetic assignment", repo)
        run_id = run["workflow_run_id"]

        def reserve(value):
            value.update(status="running", execution_contract="delegation", activations=[{
                "id": "owned-activation", "node_id": "owned-node", "role": role, "status": "running", "created_at": 0.0,
                "tasks": [{"task_id": "owned-caller", "status": "reserved"}]}])

        storage.update_run(run_id, reserve, "synthetic_authority")
        directory = storage.root / "tasks"
        store.bootstrap_catalog(directory)
        caller = SimpleNamespace(record=SimpleNamespace(task_id="owned-caller"))
        monkeypatch.setattr(lineage, "detect_caller", lambda *args, **kwargs: caller)
        monkeypatch.setattr(lineage, "detect_catalog_caller", lambda _: SimpleNamespace(caller=caller, undecidable=None))
        return storage, run_id, directory, repo, run

    return build


@pytest.mark.parametrize("role", ["builder", "orchestrator"])
async def test_managed_page_filters_against_lossless_run_metadata(managed_run, role):
    storage, run_id, directory, repo, run = managed_run(role)
    identity = storage.list_run_page(run_ids=[run_id])["items"][0]["workflow_id"]
    async def page(**filters):
        return await workflow_reads.call("list_run_page", directory=directory, limit=100, filters=filters)
    matching = [
        {"workflow_id": identity}, {"repo": str(repo.resolve())}, {"status": ["running"]}, {"scope": "root"},
        {"title_contains": "OWNED"}, {"since": "2000-01-01T00:00:00Z", "before": "2100-01-01T00:00:00Z"},
        {"workflow_id": identity, "repo": str(repo.resolve()), "scope": "root", "status": ["running", "paused"]},
    ]
    for filters in matching:
        result = await page(**filters)
        assert ids(result) == [run_id], filters
        assert result["scan_incomplete"] is False and result["total_active_count"] is None
    missing = [
        {"workflow_id": "other"}, {"repo": str(repo)+"/nope"}, {"status": ["completed"]}, {"scope": "nested"},
        {"title_contains": "absent"}, {"before": "2000-01-01T00:00:00Z"}, {"since": "2100-01-01T00:00:00Z"},
    ]
    for filters in missing:
        result = await page(**filters)
        assert result["items"] == [] and result["next_cursor"] is None and result["has_more"] is False, filters
    assert (await page())["total_active_count"] == 1
    with pytest.raises(ValueError, match="cannot be combined"):
        await workflow_reads.call("list_run_page", directory=directory, limit=1, run_ids=[run_id], filters={"scope": "root"})
    # active_only still applies.
    result = await workflow_reads.call("list_run_page", directory=directory, limit=1, active_only=True, filters={"scope": "root"})
    assert ids(result) == [run_id]


async def test_managed_worker_nodes_remain_refused(managed_run):
    storage, run_id, directory, repo, run = managed_run("node")
    with pytest.raises(ValueError, match="Worker nodes cannot inspect workflow context"):
        await workflow_reads.call("list_run_page", directory=directory, limit=1, filters={"scope": "root"})


async def test_managed_page_projects_fields(managed_run):
    storage, run_id, directory, repo, run = managed_run("orchestrator")
    full = await workflow_reads.call("list_run_page", directory=directory, limit=1)
    projected = await workflow_reads.call("list_run_page", directory=directory, limit=1, fields=["name"])
    assert projected["items"] == [{key: full["items"][0][key] for key in ("workflow_run_id", "name", "status", "created_at", "updated_at") if key in full["items"][0]}]


# ---- fields ---------------------------------------------------------------------------------

async def test_page_fields_equal_the_unprojected_keys(storage, mixed, owned_state):
    directory = owned_state / "tasks"
    full = await workflow_reads.call("list_run_page", directory=directory, limit=100)
    projected = await workflow_reads.call("list_run_page", directory=directory, limit=100, fields=["name", "workflow_id", "parent_workflow_run_id", "progress"])
    assert ids(full) == ids(projected)
    wanted = {"name", "workflow_id", "parent_workflow_run_id", "progress", "workflow_run_id", "status", "created_at", "updated_at"}
    for before, after in zip(full["items"], projected["items"]):
        assert after == {key: value for key, value in before.items() if key in wanted}
        assert {"workflow_run_id", "status", "created_at", "updated_at"} <= set(after)
    # Page metadata is untouched; related headers use the same projection.
    metadata = lambda page: {key: value for key, value in page.items() if key not in {"items", "related_headers"}}
    assert metadata(projected) == metadata(full)
    assert projected["related_headers"] and all(set(item) <= wanted for item in projected["related_headers"])


async def test_filters_and_fields_combine(storage, mixed, owned_state):
    directory = owned_state / "tasks"
    page = await workflow_reads.call("list_run_page", directory=directory, limit=100, filters={"scope": "nested"}, fields=["parent_workflow_run_id"])
    assert set(ids(page)) == {mixed["child_done"], mixed["child_failed"]}
    assert all(set(item) == {"workflow_run_id", "status", "created_at", "updated_at", "parent_workflow_run_id"} for item in page["items"])
    assert page["scan_incomplete"] is False


@pytest.mark.parametrize("fields,message", [
    (["nmae"], "did you mean 'name'"), (["status", "bogus"], "unknown run header fields"), ([], "nonempty list"),
    ("name", "must be a list"), ([1], "must be strings"),
])
async def test_invalid_fields_are_refused(owned_state, fields, message):
    with pytest.raises(ValueError, match=message):
        await workflow_reads.call("list_run_page", directory=owned_state / "tasks", limit=1, fields=fields)


def test_projection_keeps_truncation_markers_question_fields_and_diagnostics():
    item = {"workflow_run_id": "r", "status": "needs_input", "created_at": 1.0, "updated_at": 2.0, "name": "n", "summary": "s", "summary_truncated": False,
            "input_question": "q?", "input_question_items": ["- a"], "input_question_truncated": False, "pending_inputs": [{"question": "q?"}],
            "progress": {}, "goal": "dropped", "needs_direct_lookup": True, "index_blocker": "x",
            "input_decision_id": "d", "interaction_owner": "caller", "pending_input_count": 1}
    projected = workflow_responses.project_run(item, frozenset({"summary"}) | workflow_responses.RUN_REQUIRED_FIELDS)
    assert set(projected) == {"workflow_run_id", "status", "created_at", "updated_at", "summary", "summary_truncated", "input_question", "input_question_items", "input_question_truncated", "pending_inputs", "needs_direct_lookup", "index_blocker", "input_decision_id", "interaction_owner", "pending_input_count"}
    settled = workflow_responses.project_run({**item, "status": "completed"}, frozenset({"summary"}) | workflow_responses.RUN_REQUIRED_FIELDS)
    assert set(settled) == {"workflow_run_id", "status", "created_at", "updated_at", "summary", "summary_truncated", "needs_direct_lookup", "index_blocker"}


def rich_run():
    return {
        "workflow_run_id": "r", "name": "n", "kind": "workflow", "status": "needs_input", "created_at": 1.0, "updated_at": 2.0,
        "revision": 1, "goal_revision": 0, "interaction_owner": "caller", "settling": False, "execution_contract": "delegation",
        "scheduling_policy": "legacy", "input_decision_id": "d", "pending_input_count": 1, "pending_attention_count": 1,
        "monitor_controls": {"cancel": True}, "resolved_from": {"input": "r", "workflow_run_id": "r"},
        "messages": [{"status": "queued", "seq": 1, "text": "t" * 300}], "interaction_state": {"owner": "caller"},
        "definition": {"workflow_id": "wf", "name": "n"}, "approval_summary": {"pending": 0},
        "parent_link": {"workflow_run_id": "p", "root_workflow_run_id": "p", "execution_id": "e", "node_id": "n", "depth": 1},
        "orchestrator_session_owner_run_id": "o", "pending_inputs": [{"workflow_run_id": "r", "question": "q"}],
        "pending_attention": [{"workflow_run_id": "r", "reason": "x"}], "input_delivery_receipt": {"decision_id": "d"},
        "input_source": {"workflow_run_id": "r", "workflow_name": "n", "path": []}, "suspended_via_root": True,
        "dependency_tree": {}, "checklist_disposition": {"status": "done", "reason": "r"},
        "orchestrator_usage_recovery": {"decision_id": "d", "choices": [], "failure_diagnostic": {}},
        "on_demand_calls": [], "on_demand_state": {}, "input_question": "q?", "reason": "r", "wait_reason": "w",
        "failure_reason": "f", "attention_reason": "a", "summary": "s",
        "checkout_wait": {"reason": "r", "repo_path": "/r", "owner": {}}, "resources": {"names": []}, "worktree": {"path": "/w"},
        "resource_wait": {"state": "queued"},
        "activations": [{"id": "a", "status": "running", "result_error": "boom", "invocation": {"child_workflow_run_id": "c"}, "tasks": []}],
    }


def test_field_vocabularies_cover_everything_compact_can_emit():
    run = rich_run()
    for variant in (run, {**run, "status": "running"}, {**run, "kind": "builder", "draft_revision": 2}):
        keys = set(workflow_responses.compact(variant))
        markers = {key for key in keys if key.endswith("_truncated")}
        assert {key.removesuffix("_truncated") for key in markers} <= workflow_responses.COMPACT_FIELDS
        keys -= markers
        assert keys <= workflow_responses.COMPACT_FIELDS, keys - workflow_responses.COMPACT_FIELDS
        assert keys <= workflow_responses.RUN_LIST_FIELDS and keys <= workflow_responses.run_status_fields()
    assert {"repo_path", "prompt", "backends", "decisions", "sessions"} <= workflow_responses.RUN_LIST_FIELDS


def test_listing_headers_stay_within_the_list_vocabulary(storage, mixed):
    for item in storage.list_run_page()["items"]:
        extra = {key for key in item if not key.endswith("_truncated")} - workflow_responses.RUN_LIST_FIELDS
        assert extra == set(), extra


async def test_status_fields_project_after_the_full_read(storage, mixed, owned_state):
    directory = owned_state / "tasks"
    run_id = mixed["root_run"]
    full = await workflow_reads.call("status", directory=directory, run_id=run_id)
    selected = workflow_responses.validate_run_fields(["name", "progress", "interaction_state"], workflow_responses.run_status_fields(), "run status")
    projected = workflow_responses.project_run(full, selected)
    assert projected == {key: value for key, value in full.items() if key in selected}


async def test_get_workflow_status_projects_and_validates(monkeypatch):
    run = {**rich_run(), "status": "running", "input_question": None}
    run.pop("input_question")

    async def fake(action, **kwargs):
        assert action == "status"
        return run

    monkeypatch.setattr(server, "_workflow_call", fake)
    full = await server.get_workflow_status("r")
    projected = await server.get_workflow_status("r", fields=["name", "progress", "workflow_id"])
    assert projected == {key: value for key, value in full.items() if key in {"name", "progress", "workflow_id", "workflow_run_id", "status", "created_at", "updated_at", "resolved_from"}}
    needs_input = {**rich_run()}

    async def asking(action, **kwargs):
        return needs_input

    monkeypatch.setattr(server, "_workflow_call", asking)
    asked = await server.get_workflow_status("r", fields=["name"])
    assert asked["input_question"] == "q?" and "input_question_items" in asked and "pending_inputs" in asked and "progress" not in asked
    with pytest.raises(MCPError, match="did you mean 'status'"):
        await server.get_workflow_status("r", fields=["staus"])
    with pytest.raises(MCPError, match="nonempty list"):
        await server.get_workflow_status("r", fields=[])


# ---- ctl ------------------------------------------------------------------------------------

def run_ctl(monkeypatch, capsys, owned_state, *arguments):
    monkeypatch.setattr(ctl, "default_log_dir", lambda: owned_state / "tasks")
    code = ctl.main([*arguments, "--json"])
    return code, json.loads(capsys.readouterr().out)


def test_ctl_list_page_filters_and_continues(storage, clock, repos, owned_state, monkeypatch, capsys):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 4)
    hits = [make(storage, clock, repos["alpha"], workflow_id="wf-hit", status="failed", created=BASE + n) for n in (1, 9)]
    for n in range(2, 14):
        if n != 9:
            make(storage, clock, repos["alpha"], status="completed", created=BASE + n)
    store.bootstrap_catalog(owned_state / "tasks")
    code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-list-page", "--workflow-id", "wf-hit", "--status", "failed,cancelled", "--scope", "root", "--fields", "name,workflow_id")
    assert code == 0
    result, found = payload["result"], []
    for _ in range(10):
        found.extend(ids(result))
        assert all(set(item) == {"workflow_run_id", "status", "created_at", "updated_at", "name", "workflow_id"} for item in result["items"])
        if not result["has_more"]:
            break
        code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-list-page", "--workflow-id", "wf-hit", "--status", "failed,cancelled", "--scope", "root", "--fields", "name,workflow_id", "--cursor", result["next_cursor"])
        assert code == 0
        result = payload["result"]
    assert found == hits[::-1]
    assert result["scan_incomplete"] is False


def test_ctl_list_page_rejects_bad_input(storage, mixed, owned_state, monkeypatch, capsys):
    store.bootstrap_catalog(owned_state / "tasks")
    code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-list-page", "--status", "done")
    assert code == 1 and "unknown run statuses" in payload["error"]["message"]
    code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-list-page", "--fields", "nmae")
    assert code == 1 and "did you mean 'name'" in payload["error"]["message"]
    code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-list-page", "--run-ids", "abc", "--scope", "root")
    assert code == 1 and "cannot be combined" in payload["error"]["message"]
    monkeypatch.setattr(ctl, "default_log_dir", lambda: owned_state / "tasks")
    with pytest.raises(SystemExit):
        ctl.main(["workflow-list-page", "--scope", "sideways", "--json"])
    capsys.readouterr()


def test_ctl_status_fields(storage, mixed, owned_state, monkeypatch, capsys):
    store.bootstrap_catalog(owned_state / "tasks")
    run_id = mixed["root_run"]
    code, full = run_ctl(monkeypatch, capsys, owned_state, "workflow-status", run_id)
    assert code == 0
    code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-status", run_id, "--fields", "name,activations")
    assert code == 0
    assert payload["result"] == {key: value for key, value in full["result"].items() if key in {"name", "activations", "workflow_run_id", "status", "created_at", "updated_at"}}
    code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-status", run_id, "--fields", "nmae")
    assert code == 1 and "did you mean 'name'" in payload["error"]["message"]
    code, payload = run_ctl(monkeypatch, capsys, owned_state, "workflow-status", run_id, "--fields", "name", "--monitor-view")
    assert code == 1 and "--monitor-view" in payload["error"]["message"]


async def test_server_tool_end_to_end(storage, mixed, owned_state):
    store.bootstrap_catalog(owned_state / "tasks")
    page = await server.list_workflow_run_page(status=["failed"], scope="root", fields=["name"])
    assert ids(page) == [mixed["root_failed"]]
    assert set(page["items"][0]) == {"workflow_run_id", "status", "created_at", "updated_at", "name"}
    assert page["scan_incomplete"] is False and page["total_active_count"] is None
    with pytest.raises(MCPError, match="unknown run statuses"):
        await server.list_workflow_run_page(status=["bogus"])
    with pytest.raises(MCPError, match="cannot be combined"):
        await server.list_workflow_run_page(scope="root", run_ids=[mixed["root_failed"]])
    with pytest.raises(MCPError, match="did you mean 'name'"):
        await server.list_workflow_run_page(fields=["nmae"])
    status = await server.get_workflow_status(mixed["root_failed"], fields=["name"])
    assert set(status) == {"workflow_run_id", "status", "created_at", "updated_at", "name"}


# ---- review round 1 -------------------------------------------------------------------------

def test_a_page_that_fills_is_not_scan_incomplete(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 7)
    runs = {n: make(storage, clock, repos["alpha"], workflow_id="wf-hit" if n in {20, 7, 3} else "wf-miss", created=BASE + n) for n in range(1, 21)}
    # Window is rows 20..14. Newest match early in the window; exactly one match fills the page.
    first = storage.list_run_page(limit=1, filters={"workflow_id": "wf-hit"})
    assert ids(first) == [runs[20]]
    assert first["scan_incomplete"] is False and first["has_more"] is True and first["next_cursor"]
    # A single match at the window edge: it is the last examined row.
    second = storage.list_run_page(limit=1, filters={"workflow_id": "wf-hit"}, cursor=first["next_cursor"])
    assert ids(second) == [runs[7]] and second["scan_incomplete"] is False and second["has_more"] is True
    decoded = json.loads(__import__("base64").urlsafe_b64decode(second["next_cursor"]))
    assert decoded["id"] == runs[7]
    third = storage.list_run_page(limit=1, filters={"workflow_id": "wf-hit"}, cursor=second["next_cursor"])
    assert ids(third) == [runs[3]] and third["scan_incomplete"] is False and third["has_more"] is False
    assert walk(storage, limit=1, filters={"workflow_id": "wf-hit"})[0] == [runs[20], runs[7], runs[3]]


def test_exactly_limit_matches_early_in_window_with_history_beyond(storage, clock, repos, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 7)
    runs = {n: make(storage, clock, repos["alpha"], workflow_id="wf-hit" if n == 19 else "wf-miss", created=BASE + n) for n in range(1, 21)}
    page = storage.list_run_page(limit=1, filters={"workflow_id": "wf-hit"})
    assert ids(page) == [runs[19]] and page["scan_incomplete"] is False and page["has_more"] is True and page["next_cursor"]
    rest = storage.list_run_page(limit=1, filters={"workflow_id": "wf-hit"}, cursor=page["next_cursor"])
    assert rest["items"] == [] and rest["scan_incomplete"] is True


def seed_tied(storage, count, stamp=BASE):
    index = catalog.Catalog(storage.runs, ".json", layout=storage.state_layout)
    with index.connect() as db:
        db.executemany("INSERT INTO entries VALUES (?,?,0,NULL,?)", [(f"t{n:06}", stamp, json.dumps({"workflow_run_id": f"t{n:06}", "status": "completed"})) for n in range(count)])
        db.executemany("INSERT INTO run_meta VALUES (?,?,?,?,?)", [(f"t{n:06}", "wf-hit" if n % 7 == 0 else "wf-miss", "/r", "n", 1) for n in range(count)])
    return index


@pytest.fixture
def frozen_catalog(monkeypatch):
    """Rows are seeded directly: no discovery, no source stats."""
    monkeypatch.setattr(catalog.Catalog, "bootstrap", lambda self, db, loader: False)
    monkeypatch.setattr(catalog.Catalog, "_changed", lambda self, db, identifier: False)


@pytest.mark.parametrize("window", [2000, 300])
def test_tied_timestamps_page_every_row_exactly_once(storage, frozen_catalog, monkeypatch, window):
    monkeypatch.setattr(catalog, "SCAN_ROWS", window)
    seed_tied(storage, 3000)
    expected = [f"t{n:06}" for n in range(2999, -1, -1) if n % 7 == 0]
    found, pages = walk(storage, limit=25, filters={"workflow_id": "wf-hit"})
    assert found == expected
    assert not any(page["scan_incomplete"] for page in pages)
    everything, _ = walk(storage, limit=100, filters={"scope": "root"})
    assert everything == [f"t{n:06}" for n in range(2999, -1, -1)]


def measure(storage, rows, monkeypatch, filters, calls=3):
    """Instructions needed by the window and page queries of the third call over tied stamps."""
    import sqlite3
    seed_tied(storage, rows)
    statements = []
    connect = catalog.Catalog.connect

    def traced(self):
        db = connect(self)
        db.set_trace_callback(statements.append)
        return db

    monkeypatch.setattr(catalog.Catalog, "connect", traced)
    cursor = None
    for _ in range(calls):
        statements.clear()
        page = storage.list_run_page(cursor=cursor, filters=filters)
        cursor = page["next_cursor"]
    monkeypatch.setattr(catalog.Catalog, "connect", connect)
    queries = [text for text in statements if text.startswith(("SELECT stamp,id FROM entries", "SELECT id,stamp,payload FROM ("))]
    assert len(queries) == 2
    total = 0
    with sqlite3.connect(catalog.catalog_path(storage.runs)) as db:
        for text in queries:
            plan = " ".join(row[3] for row in db.execute("EXPLAIN QUERY PLAN " + text))
            assert "SEARCH" in plan and "(stamp,id)" in plan, plan
            counter = [0]
            db.set_progress_handler(lambda: counter.__setitem__(0, counter[0] + 1) or 0, 50)
            db.execute(text).fetchall()
            db.set_progress_handler(None, 0)
            total += counter[0]
    return total


@pytest.mark.parametrize("filters,calls", [
    ({"workflow_id": "wf-none"}, 3),
    ({"status": ["failed"]}, 3),
    ({"status": ["failed"], "since": iso(BASE - 10)}, 3),
    ({"status": ["failed"], "before": iso(BASE + 10)}, 3),
    ({"status": ["failed"], "since": iso(BASE - 10), "before": iso(BASE + 10)}, 3),
    ({"workflow_id": "wf-none", "since": iso(BASE - 10), "before": iso(BASE + 10)}, 4),
])
def test_tied_timestamps_keep_window_work_bounded(storage, frozen_catalog, owned_state, tmp_path, monkeypatch, clock, filters, calls):
    small = measure(storage, 8000, monkeypatch, filters, calls)
    other_root = tmp_path / "state2"
    with state_ownership.state_session(resolve_layout(root=other_root)):
        large = measure(workflows.WorkflowStore(root=other_root), 30000, monkeypatch, filters, calls)
    assert small > 0 and large < small * 1.5, (small, large)


def test_budget_placeholder_does_not_keep_run_meta(storage, clock, repos, monkeypatch):
    run = make(storage, clock, repos["alpha"], workflow_id="wf-a")
    storage.list_run_page()
    assert run in run_meta_rows(storage)
    real = catalog.bound_header

    def over_budget(header):
        if header.get("workflow_run_id") == run:
            return {"workflow_run_id": run, "status": "unknown", "needs_direct_lookup": True, "index_blocker": "header_metadata_budget"}
        return real(header)

    monkeypatch.setattr(catalog, "bound_header", over_budget)
    storage.update_run(run, lambda value: value.update(summary="x"), "touch")
    assert run not in run_meta_rows(storage)
    assert names(storage, workflow_id="wf-a") == set()


async def test_fields_keep_the_related_header_join(storage, mixed, owned_state):
    page = await workflow_reads.call("list_run_page", directory=owned_state / "tasks", limit=100, fields=["name"])
    child = next(item for item in page["items"] if item["workflow_run_id"] == mixed["child_done"])
    assert child["parent_workflow_run_id"] == mixed["root_done"]
    assert any(item["workflow_run_id"] == mixed["root_done"] for item in page["related_headers"])


async def test_mcp_status_fields_use_the_compact_vocabulary(monkeypatch):
    async def fake(action, **kwargs):
        return rich_run()

    monkeypatch.setattr(server, "_workflow_call", fake)
    with pytest.raises(MCPError, match="unknown run status fields"):
        await server.get_workflow_status("r", fields=["definition"])
    assert (await server.get_workflow_status("r", fields=["resolved_from"]))["resolved_from"]


@pytest.mark.parametrize("filters", [
    {"workflow_id": "wf-hit", "since": iso(BASE - 10)}, {"workflow_id": "wf-hit", "before": iso(BASE + 10)},
    {"workflow_id": "wf-hit", "since": iso(BASE - 10), "before": iso(BASE + 10)},
])
def test_tied_timestamp_filters_page_every_match_once(storage, frozen_catalog, monkeypatch, filters):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 500)
    seed_tied(storage, 3000)
    found, _ = walk(storage, limit=30, filters=filters)
    assert found == [f"t{n:06}" for n in range(2999, -1, -1) if n % 7 == 0]


def test_tied_timestamps_sparse_filter_reports_scan_incomplete(storage, frozen_catalog, monkeypatch):
    monkeypatch.setattr(catalog, "SCAN_ROWS", 300)
    seed_tied(storage, 3000)
    found, pages = walk(storage, limit=100, filters={"workflow_id": "wf-hit"})
    assert len(found) == len(set(found)) == 429
    assert any(page["scan_incomplete"] for page in pages)
