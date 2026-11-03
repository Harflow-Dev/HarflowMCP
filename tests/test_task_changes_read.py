"""`task-changes` reader: listing and text modes, cursors, capture state and reader I/O."""
from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from task_changes_support import TASK, make_document, make_repository, make_row, seed_task, write_document

from harflow_mcp import identity, state_ownership, store, task_changes_document as shape, task_changes_read as read
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture
def world(tmp_path):
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield type("World", (), {"tasks": layout.tasks_directory, "root": tmp_path})


def record(world, **seed):
    return seed_task(world.tasks, seed.pop("repositories", [{"repository_root": "/r"}]), **seed)


def result(world, task=None, **options):
    return read.task_changes_result(world.tasks, task or store.read(world.tasks, TASK, include_prompt=False), **options)


def stored(world, repositories, **options):
    seed_task(world.tasks, [{"repository_root": "/r"}])
    write_document(world.tasks, make_document(repositories, **options))
    return store.read(world.tasks, TASK, include_prompt=False)


def paths(page):
    return [row["path"] for repository in page["repositories"] for scope in repository["scopes"].values() for row in scope["rows"]]


def wire_size(page):
    return read._document_bytes(page)


# --- capture state ----------------------------------------------------------------------------


def test_stored_document_state_is_served(world):
    task = stored(world, [make_repository("/r", [make_row("a", b"diff a")])], capture={"state": "incomplete", "reason": "capture_timeout"})
    page = result(world, task)
    assert page["capture"] == {"state": "incomplete", "reason": "capture_timeout"} and page["mode"] == "list"
    repository = page["repositories"][0]
    assert repository["attribution"] == shape.ATTRIBUTION and repository["scopes"]["working_tree"]["counts_and_patch_separate"] is True
    row = repository["scopes"]["committed_range"]["rows"][0]
    assert row["content"] == {"state": "available", "reason": None, "bytes": 6} and "text_b64" not in row
    assert repository["scopes"]["committed_range"]["row_count"] == 1


@pytest.mark.parametrize("seed,owner,expected", [
    ({"status": "running", "exit_code": None, "finished_at": None}, "alive", ("pending", "task_running")),
    ({"status": "failed", "exit_code": None}, "dead", ("not_captured", "outcome_unobserved")),
    ({"repositories": None}, "dead", ("available", "no_repository")),
    ({"repositories": []}, "alive", ("available", "no_repository")),
    ({}, "dead", ("not_captured", "capture_missing")),
    ({}, "alive", ("pending", "capture_settling")),
])
def test_capture_state_without_a_document(world, monkeypatch, seed, owner, expected):
    monkeypatch.setattr(read, "_supervisor_owner", lambda *_: owner)
    page = result(world, record(world, **seed))
    assert (page["capture"]["state"], page["capture"]["reason"]) == expected
    assert page["repositories"] == [] and page["has_more"] is False and page["next_cursor"] is None


@pytest.mark.parametrize("age,expected", [(5, ("pending", "capture_settling")), (120, ("not_captured", "capture_missing"))])
def test_undecidable_owner_uses_a_grace_period(world, monkeypatch, age, expected):
    monkeypatch.setattr(read, "_supervisor_owner", lambda *_: "undecidable")
    finished = (datetime.now(timezone.utc) - timedelta(seconds=age)).isoformat()
    page = result(world, record(world, finished_at=finished))
    assert (page["capture"]["state"], page["capture"]["reason"]) == expected


def test_owner_is_taken_from_the_launch_receipt(world, monkeypatch):
    from harflow_mcp import task_supervision
    owner = {"pid": 4242, "start_time": "Wed Jan  1 00:00:00 2026", "markers": []}
    verdicts = {}
    monkeypatch.setattr(identity, "identity_check", lambda candidate: verdicts.get(candidate["pid"], "undecidable"))
    monkeypatch.setattr(task_supervision, "read_launch", lambda *_: {"owner": owner})
    assert read._supervisor_owner(world.tasks, TASK) == "undecidable"
    verdicts[4242] = "dead"
    assert read._supervisor_owner(world.tasks, TASK) == "dead"
    monkeypatch.setattr(task_supervision, "read_launch", lambda *_: None)
    assert read._supervisor_owner(world.tasks, TASK) == "undecidable"

    def broken(*_):
        raise ValueError("unreadable")
    monkeypatch.setattr(task_supervision, "read_launch", broken)
    assert read._supervisor_owner(world.tasks, TASK) == "undecidable"


def test_invalid_oversized_special_and_foreign_documents(world):
    task = record(world)
    path = world.tasks / f"{TASK}{shape.SUFFIX}"
    expected = {"state": "unavailable", "reason": "changes_document_invalid"}
    path.write_text("{not json")
    assert result(world, task)["capture"] == expected
    document = make_document([make_repository("/r", [make_row("a")])])
    document["repositories"][0]["scopes"]["committed_range"]["row_count"] = 9
    path.write_text(json.dumps(document))
    assert result(world, task)["capture"] == expected
    path.write_text(json.dumps(make_document([], task_id="another")))
    assert result(world, task)["capture"] == expected
    path.write_bytes(b" " * (shape.MAX_DOCUMENT_BYTES + 1))
    assert result(world, task)["capture"] == expected
    path.unlink()
    os.mkfifo(path)
    assert result(world, task)["capture"] == expected
    path.unlink()
    target = world.root / "elsewhere.json"
    target.write_text(json.dumps(make_document([])))
    os.symlink(target, path)
    assert result(world, task)["capture"] == expected


def test_orphaned_temporary_file_is_never_read_as_the_document(world, monkeypatch):
    monkeypatch.setattr(read, "_supervisor_owner", lambda *_: "dead")
    task = record(world)
    (world.tasks / f"{TASK}{shape.SUFFIX}.0123456789abcdef.tmp").write_text(json.dumps(make_document([])))
    assert result(world, task)["capture"]["reason"] == "capture_missing"


def test_failure_documents_are_valid(world):
    task = stored(world, [], capture={"state": "unavailable", "reason": "capture_too_large"})
    page = result(world, task)
    assert page["capture"] == {"state": "unavailable", "reason": "capture_too_large"} and page["repositories"] == []


# --- listing ----------------------------------------------------------------------------------


def three_repositories():
    return [make_repository("/a", [make_row("a1", b"x"), make_row("a2", b"y")], [make_row("w1", b"z")]),
            make_repository("/b", [], [make_row("b1"), make_row("b2")]), make_repository("/c", [make_row("c1")])]


def test_listing_pages_cover_every_row_once_in_order(world):
    task = stored(world, three_repositories())
    collected, cursor, pages = [], None, 0
    while True:
        page = result(world, task, limit=2, cursor=cursor)
        collected += paths(page)
        pages += 1
        assert [repository["repository_root"] for repository in page["repositories"]] == ["/a", "/b", "/c"]
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
        assert len(paths(page)) == 2
    assert collected == ["a1", "a2", "w1", "b1", "b2", "c1"] and pages == 3 and cursor is None


def test_listing_page_fits_the_document_budget_with_long_paths(world):
    rows = [make_row(f"{index:04d}-" + "p" * 4000) for index in range(400)]
    task = stored(world, [make_repository("/r", rows)])
    seen, cursor = 0, None
    while True:
        page = result(world, task, limit=500, cursor=cursor)
        count = len(paths(page))
        assert 1 <= count < 400 and wire_size(page) + read.FIT_RESERVE_BYTES <= read.DOCUMENT_MAX_BYTES
        seen += count
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    assert seen == 400


def test_a_single_maximum_path_row_still_fits(world):
    rows = [make_row("q" * shape.MAX_PATH_BYTES)]
    page = result(world, stored(world, [make_repository("/r", rows)]), limit=500)
    assert len(paths(page)) == 1 and wire_size(page) < read.DOCUMENT_MAX_BYTES


# --- cursors ----------------------------------------------------------------------------------


def test_cursor_errors(world):
    task = stored(world, three_repositories())
    good = result(world, task, limit=1)["next_cursor"]
    for bad in ("", "!!!", "e30", "A" * 2049, base64.urlsafe_b64encode(b'{"g":"py-changes-1"}').decode()):
        with pytest.raises(read.ChangesCursorInvalid) as raised:
            result(world, task, cursor=bad)
        assert raised.value.code == "changes_cursor_invalid"
    other = seed_task(world.tasks, [{"repository_root": "/r"}], task_id="other-task")
    write_document(world.tasks, make_document(three_repositories(), task_id="other-task"), task_id="other-task")
    with pytest.raises(read.ChangesCursorInvalid):
        result(world, other, cursor=good)
    payload = json.loads(base64.urlsafe_b64decode(good + "=" * (-len(good) % 4)))
    forged = base64.urlsafe_b64encode(json.dumps({**payload, "e": [9, 0, 0]}).encode()).decode().rstrip("=")
    with pytest.raises(read.ChangesCursorInvalid):
        result(world, task, cursor=forged)
    generation = base64.urlsafe_b64encode(json.dumps({**payload, "g": "py-changes-0"}).encode()).decode().rstrip("=")
    with pytest.raises(read.ChangesCursorExpired):
        result(world, task, cursor=generation)


def test_cursor_expires_when_the_document_changes_or_goes(world):
    task = stored(world, three_repositories())
    cursor = result(world, task, limit=1)["next_cursor"]
    write_document(world.tasks, make_document(three_repositories()[:2]))
    with pytest.raises(read.ChangesCursorExpired) as raised:
        result(world, task, cursor=cursor)
    assert raised.value.code == "changes_cursor_expired" and raised.value.data == {"reload": "first"}
    (world.tasks / f"{TASK}{shape.SUFFIX}").unlink()
    with pytest.raises(read.ChangesCursorExpired):
        result(world, task, cursor=cursor)


# --- text -------------------------------------------------------------------------------------


def selector(path, scope="committed_range", root="/r"):
    return read.Selector(root, scope, path if isinstance(path, bytes) else path.encode())


def drain(world, task, first):
    pieces, page = [], first
    while True:
        pieces.append(page)
        if not page["has_more"]:
            return pieces
        page = result(world, task, cursor=page["next_cursor"])


def joined(pages):
    if pages[0]["text"] is not None:
        return "".join(page["text"] for page in pages).encode()
    return b"".join(base64.b64decode(page["text_b64"]) for page in pages)


def test_text_of_a_small_row(world):
    task = stored(world, [make_repository("/r", [make_row("a", b"diff --git a/a b/a\n+hi\n")])])
    page = result(world, task, selector=selector("a"))
    assert page["mode"] == "text" and page["text"] == "diff --git a/a b/a\n+hi\n" and page["text_b64"] is None
    assert (page["offset"], page["returned_bytes"], page["total_bytes"], page["has_more"], page["next_cursor"]) == (0, 23, 23, False, None)
    assert (page["repository_root"], page["scope"], page["path"], page["status"]) == ("/r", "committed_range", "a", "modified")


def test_text_requests_for_rows_without_text(world):
    task = stored(world, [make_repository("/r", [make_row("a")], [make_row("w", state="binary")])])
    page = result(world, task, selector=selector("a"))
    assert (page["text"], page["text_b64"], page["total_bytes"], page["has_more"]) == ("", None, 0, False)
    assert result(world, task, selector=selector("w", "working_tree"))["content"]["state"] == "binary"


def test_unknown_entries_are_not_found(world):
    task = stored(world, [make_repository("/r", [make_row("a", b"x")])])
    for given in (selector("missing"), selector("a", "working_tree"), selector("a", root="/other"), selector("a", scope="nope")):
        with pytest.raises(read.ChangesEntryNotFound) as raised:
            result(world, task, selector=given)
        assert raised.value.code == "changes_entry_not_found"
    with pytest.raises(read.ChangesEntryNotFound):
        result(world, task, selector=selector("a", root="/r/"))


def test_text_without_a_document_is_not_found(world, monkeypatch):
    monkeypatch.setattr(read, "_supervisor_owner", lambda *_: "alive")
    with pytest.raises(read.ChangesEntryNotFound):
        result(world, record(world), selector=selector("a"))


def test_heavily_escaped_text_is_chunked_to_fit(world):
    text = bytes(range(1, 32)) * 2000  # 62 KB; each control character escapes to six bytes
    task = stored(world, [make_repository("/r", [make_row("a", text)])])
    pages = drain(world, task, result(world, task, selector=selector("a")))
    assert len(pages) > 1
    for page in pages:
        assert wire_size(page) + read.FIT_RESERVE_BYTES <= read.DOCUMENT_MAX_BYTES
    assert joined(pages) == text and pages[-1]["offset"] + pages[-1]["returned_bytes"] == len(text)
    assert [page["offset"] for page in pages] == sorted(page["offset"] for page in pages)


def test_chunks_cut_on_character_boundaries(world, monkeypatch):
    monkeypatch.setattr(read, "DOCUMENT_MAX_BYTES", 2500)
    text = ("é€😀" * 400).encode()
    task = stored(world, [make_repository("/r", [make_row("a", text)])])
    pages = drain(world, task, result(world, task, selector=selector("a")))
    assert len(pages) > 3 and all(page["text"] is not None for page in pages)
    assert joined(pages) == text


def test_text_that_is_not_valid_utf8_is_served_as_base64(world, monkeypatch):
    monkeypatch.setattr(read, "DOCUMENT_MAX_BYTES", 2500)
    data = b"\xff\xfe" + bytes(range(256)) * 20
    task = stored(world, [make_repository("/r", [make_row("a", data)])])
    pages = drain(world, task, result(world, task, selector=selector("a")))
    assert len(pages) > 1 and all(page["text"] is None and page["text_b64"] for page in pages)
    assert joined(pages) == data


def test_text_cursor_errors(world, monkeypatch):
    monkeypatch.setattr(read, "DOCUMENT_MAX_BYTES", 2500)
    task = stored(world, [make_repository("/r", [make_row("a", ("é" * 2000).encode())])])
    first = result(world, task, selector=selector("a"))
    cursor = first["next_cursor"]
    payload = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))

    def forge(**changes):
        return base64.urlsafe_b64encode(json.dumps({**payload, **changes}).encode()).decode().rstrip("=")
    with pytest.raises(read.ChangesCursorInvalid):
        result(world, task, cursor=forge(o=payload["o"] + 1))  # inside a two-byte character
    with pytest.raises(read.ChangesCursorInvalid):
        result(world, task, cursor=forge(o=4001))  # past the end of the stored text
    with pytest.raises(read.ChangesCursorInvalid):
        result(world, task, cursor=forge(e=[0, 0, 5]))
    write_document(world.tasks, make_document([make_repository("/r", [make_row("a", b"changed")])]))
    with pytest.raises(read.ChangesCursorExpired):
        result(world, task, cursor=cursor)
