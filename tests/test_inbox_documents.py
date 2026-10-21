import json

import pytest

from harflow_mcp import inbox, inbox_documents
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session


@pytest.fixture
def owned_root(tmp_path):
    with state_session(resolve_layout(root=tmp_path)):
        tasks = tmp_path / "tasks"
        tasks.mkdir()
        yield tasks


def _message(text="hello"):
    return {"id": "message-1", "text": text}


def test_codec_round_trip_preserves_payload_and_additive_fields():
    payload = {"id": "message-1", "text": "  hi\n世界  ", "extra": {"source": "test"}}
    document = inbox_documents.encode(payload, "task-1")
    assert document["format"] == inbox_documents.INBOX_FORMAT
    assert inbox_documents.decode(document, "task-1") == payload


@pytest.mark.parametrize(
    "mutator",
    [
        lambda d: d.update(product="foreign"),
        lambda d: d.update(format="foreign.format"),
        lambda d: d.update(v=2),
        lambda d: d.update(task_id="other"),
        lambda d: d["payload"].update(id=3),
        lambda d: d.update(kind="seal"),
    ],
)
def test_decode_rejects_foreign_or_mismatched_documents(mutator):
    document = inbox_documents.encode(_message(), "task-1")
    mutator(document)
    with pytest.raises(inbox_documents.InboxDocumentError):
        inbox_documents.decode(document, "task-1")


@pytest.mark.parametrize("field, value", [("v", True), ("product_version", "2.0.0")])
def test_decode_rejects_incompatible_version_identity(field, value):
    document = inbox_documents.encode(_message(), "task-1")
    document[field] = value
    with pytest.raises(inbox_documents.InboxDocumentError):
        inbox_documents.decode(document, "task-1")


def test_codec_accepts_minimal_message_and_seal():
    assert inbox_documents.decode(inbox_documents.encode(_message(), "task-1"), "task-1")["id"] == "message-1"
    seal = {"closed": True, "at": "now", "extra": [1, 2]}
    assert inbox_documents.decode(inbox_documents.encode(seal, "task-1"), "task-1") == seal


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_codec_rejects_nonfinite_additive_values(bad):
    with pytest.raises(inbox_documents.InboxDocumentError):
        inbox_documents.encode({"id": "message-1", "text": "ok", "bad": bad}, "task-1")


def test_append_default_str_coercion_is_preserved(owned_root):
    class Marker:
        def __str__(self):
            return "marker-value"

    path = inbox.inbox_path(owned_root, "task-1")
    inbox.append_locked(owned_root, "task-1", {"id": "message-1", "text": "ok", "extra": Marker()})
    raw = json.loads(path.read_text())
    assert inbox_documents.decode(raw, "task-1")["extra"] == "marker-value"


def test_failed_candidate_does_not_touch_existing_torn_bytes(owned_root, monkeypatch):
    path = inbox.inbox_path(owned_root, "task-1")
    original = b'{"id":"torn","text":"unfinished"'
    path.write_bytes(original)
    monkeypatch.setattr(inbox_documents, "encode", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad candidate")))
    with pytest.raises(OSError):
        inbox.append_locked(owned_root, "task-1", _message())
    assert path.read_bytes() == original


def test_real_nonfinite_candidate_fails_before_separator_or_inode_change(owned_root):
    path = inbox.inbox_path(owned_root, "task-1")
    original = b'{"id":"torn","text":"unfinished"'
    path.write_bytes(original)
    inode = path.stat().st_ino
    with pytest.raises(OSError):
        inbox.append_locked(owned_root, "task-1", {"id": "message-1", "text": "ok", "bad": float("nan")})
    assert path.read_bytes() == original
    assert path.stat().st_ino == inode


def test_physical_closed_marker_is_presence_only(owned_root):
    closed = inbox.closed_path(owned_root, "task-1")
    closed.write_bytes(b"not json")
    assert inbox.is_closed(owned_root, "task-1") is True


def test_qualified_trailing_seal_without_newline_closes_but_is_not_a_message(owned_root):
    path = inbox.inbox_path(owned_root, "task-1")
    path.write_text(json.dumps(inbox_documents.encode({"closed": True}, "task-1")))
    assert inbox.is_closed(owned_root, "task-1") is True
    assert inbox.read_new(owned_root, "task-1", 0) == ([], 0)


def test_raw_or_foreign_seal_does_not_close(owned_root):
    path = inbox.inbox_path(owned_root, "task-1")
    raw = json.dumps({"closed": True})
    foreign = json.dumps({**inbox_documents.encode({"closed": True}, "task-1"), "task_id": "other"})
    path.write_text(raw + "\n" + foreign + "\n")
    assert inbox.is_closed(owned_root, "task-1") is False


def test_mixed_rejected_rows_advance_to_valid_message_and_leave_torn_tail(owned_root):
    path = inbox.inbox_path(owned_root, "task-1")
    valid = inbox_documents.encode(_message("valid"), "task-1")
    foreign = {**valid, "task_id": "other"}
    raw = _message("raw")
    complete = b"\xff\xfe\n"
    prefix = b"\n".join(json.dumps(row).encode() for row in (foreign, raw)) + b"\n" + complete
    prefix += (json.dumps(valid) + "\n").encode()
    tail = b'{"product":"harflow"'
    path.write_bytes(prefix + tail)
    messages, offset = inbox.read_new(owned_root, "task-1", 0)
    assert [row["text"] for row in messages] == ["valid"]
    assert offset == len(prefix)


def test_recursive_malformed_rows_are_skipped_by_read_and_pending(owned_root):
    path = inbox.inbox_path(owned_root, "task-1")
    valid = inbox_documents.encode(_message("valid"), "task-1")
    recursive = ("[" * 2000 + "]" * 2000).encode() + b"\n"
    path.write_bytes(recursive + (json.dumps(valid) + "\n").encode())
    messages, offset = inbox.read_new(owned_root, "task-1", 0)
    assert [row["text"] for row in messages] == ["valid"]
    assert offset == len(path.read_bytes())
    assert [row["text"] for row in inbox.pending_messages(owned_root, "task-1")] == ["valid"]
