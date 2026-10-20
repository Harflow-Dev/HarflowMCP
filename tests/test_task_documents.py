"""Contract tests for qualified persisted task metadata."""

from __future__ import annotations

import copy
import json
from dataclasses import asdict
from pathlib import Path

import pytest

from harflow_mcp import store, task_documents
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout


TASK_ID = "task-1"


def record(**overrides) -> store.TaskRecord:
    values = {
        "task_id": TASK_ID,
        "backend": "claude",
        "session_id": "session-1",
        "repo_path": "/repo",
        "started_at": "2026-10-19T00:00:00+00:00",
        "markers": ["session-1"],
        "prompt": "assignment",
    }
    return store.TaskRecord(**(values | overrides))


def document(**overrides) -> dict:
    return task_documents.encode(asdict(record(**overrides)))


def decode(payload: dict | None = None, *, task_id: str = TASK_ID):
    return task_documents.decode(payload or document(), task_id, store.TaskRecord)


@pytest.fixture
def task_dir(tmp_path: Path):
    root = tmp_path / "state"
    directory = root / "tasks"
    with state_ownership.state_session(resolve_layout(root=root)):
        directory.mkdir(parents=True)
        yield directory


def test_encode_stamps_exact_task_envelope_and_decode_round_trips() -> None:
    payload = document()

    assert {key: payload[key] for key in ("product", "format", "v")} == {
        "product": "harflow",
        "format": "harflow.task",
        "v": 1,
    }
    assert payload["product_version"].split(".")[:2] == ["1", "0"]
    assert decode() == record()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("product", "foreign"),
        ("format", "harflow.event"),
        ("v", True),
        ("v", 2),
        ("product_version", "1.1.0"),
        ("product_version", "1.0"),
        ("product_version", "1.0.0-rc.1"),
    ],
)
def test_reserved_identity_mismatch_is_a_typed_refusal(field, value) -> None:
    payload = document()
    payload[field] = value

    with pytest.raises(task_documents.TaskDocumentError) as caught:
        decode(payload)

    error = caught.value
    assert error.code == "task_incompatible"
    assert error.task_id == TASK_ID
    assert error.as_dict() == {"task_id": TASK_ID, "code": error.code, "reason": error.reason}


def test_patch_version_and_unknown_additive_fields_are_compatible() -> None:
    payload = document()
    payload["product_version"] = "1.0.0+fixture"
    payload["future_field"] = {"preserve": "as input"}

    loaded = decode(payload)

    assert loaded == record()


@pytest.mark.parametrize("product_version", ["1.0.1", "1.0.99+fixture"])
def test_both_patch_directions_and_build_metadata_round_trip(product_version: str) -> None:
    payload = document()
    payload["product_version"] = product_version

    assert decode(payload) == record()


@pytest.mark.parametrize(
    "changes",
    [
        {"task_id": "other"},
        {"backend": 7},
        {"max_turns": True},
        {"markers": ["ok", 3]},
        {"owner": {"pid": 4, "start_time": None}},
        {"extra_writable_paths": [{"requested": "/tmp/x"}]},
    ],
)
def test_malformed_known_payload_fields_refuse_without_coercion(changes) -> None:
    payload = document()
    payload.update(changes)

    with pytest.raises(task_documents.TaskDocumentError):
        decode(payload)


def test_nested_producer_shapes_are_validated_structurally() -> None:
    owner = {"pid": 123, "start_time": "42", "markers": ["harflow"]}
    grant = {"requested": "/tmp/grant", "path": "/tmp/grant", "kind": "directory", "device": 1, "inode": 2}
    enforcement = {
        "freedom": "write_in_repo", "mechanism": "sandbox", "os_enforced": True,
        "writes_confined": True, "writable_roots": ["/repo"], "commit_push_blocked": True,
        "direct_commit_commands_denied": True, "publish_attempts_allowed_by_polybridge": False,
        "network_access": "blocked", "caveats": [], "extra_writable_paths": [grant],
        "extra_path_certification": "certified",
    }
    outcome = {
        "base_commit": "abc", "head_commit": "def", "new_commits": ["def"],
        "new_commits_truncated": False, "pushed": False, "attribution": "agent",
        "structured_evidence_complete": True,
    }
    payload = document(owner=owner, extra_writable_paths=[grant], enforcement=enforcement, outcome=outcome,
                       failure_diagnostic={"category": "usage_limit", "reason": "quota", "source": "test", "reset_at": "later"},
                       git_baseline={"head": "abc", "branch": "main", "repository_root": "/repo", "working_tree_status": "clean", "provenance": "test", "error": None})

    loaded = decode(payload)

    assert loaded.owner == owner
    assert loaded.enforcement == enforcement
    assert loaded.extra_writable_paths == [grant]
    assert loaded.outcome == outcome


def test_enforcement_requires_all_authority_fields() -> None:
    payload = document(enforcement={"freedom": "write_in_repo", "mechanism": "sandbox"})

    with pytest.raises(task_documents.TaskDocumentError):
        decode(payload)


def test_store_distinguishes_missing_malformed_and_unavailable(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert store.read(task_dir, TASK_ID) is None
    path = store.record_path(task_dir, TASK_ID)
    path.write_text("{\"product\": \"foreign\"}", encoding="utf-8")

    with pytest.raises(task_documents.TaskDocumentError) as malformed:
        store.read(task_dir, TASK_ID)
    assert malformed.value.code == "task_incompatible"

    monkeypatch.setattr(Path, "read_text", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("offline")))
    with pytest.raises(task_documents.TaskDocumentError) as unavailable:
        store.read(task_dir, TASK_ID)
    assert unavailable.value.code == "task_unavailable"


def test_refused_write_does_not_replace_metadata_or_prompt_sidecar(task_dir: Path) -> None:
    original = record(prompt="x" * 50000)
    assert store.write_landed(task_dir, original)
    metadata = store.record_path(task_dir, TASK_ID).read_bytes()
    sidecars = {path.name: path.read_bytes() for path in task_dir.glob(f"{TASK_ID}.prompt.*.txt")}
    store.record_path(task_dir, TASK_ID).write_text(json.dumps({"product": "foreign"}), encoding="utf-8")

    assert not store.write_landed(task_dir, record(title="must not land"))
    assert store.last_write_error().code == "task_incompatible"
    assert store.record_path(task_dir, TASK_ID).read_text(encoding="utf-8") == json.dumps({"product": "foreign"})
    assert {path.name: path.read_bytes() for path in task_dir.glob(f"{TASK_ID}.prompt.*.txt")} == sidecars
    assert metadata != store.record_path(task_dir, TASK_ID).read_bytes()


def test_writer_validates_parent_before_following_prompt_sidecar(task_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    original = record(prompt="x" * 50000)
    assert store.write_landed(task_dir, original)
    path = store.record_path(task_dir, TASK_ID)
    path.write_text(json.dumps({"product": "foreign"}), encoding="utf-8")
    sidecar_reads: list[str] = []
    read_bytes = Path.read_bytes

    def observe(path: Path, *args, **kwargs):
        if ".prompt." in path.name:
            sidecar_reads.append(path.name)
        return read_bytes(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", observe)
    assert not store.write_landed(task_dir, record(prompt="y" * 50000))
    assert sidecar_reads == []


def test_scan_reports_refusals_and_strict_read_all_does_not_drop_them(task_dir: Path) -> None:
    assert store.write_landed(task_dir, record())
    bad = store.record_path(task_dir, "bad")
    bad.write_text(json.dumps({"product": "foreign"}), encoding="utf-8")

    scan = store.scan(task_dir)
    assert [item.task_id for item in scan.records] == [TASK_ID]
    assert set(scan.refusals) == {"bad"}
    assert scan.history_incomplete
    assert scan.diagnostics()["refused_tasks"][0]["code"] == "task_incompatible"
    with pytest.raises(task_documents.TaskDocumentError):
        store.read_all(task_dir)


def test_scan_enumeration_failure_is_typed_unavailable_not_empty_history(
    task_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def deny_enumeration(path):
        raise PermissionError("directory listing denied")

    monkeypatch.setattr(store.os, "scandir", deny_enumeration)

    with pytest.raises(task_documents.TaskDocumentError) as caught:
        store.scan(task_dir)

    assert caught.value.task_id == "*"
    assert caught.value.code == "task_unavailable"
    assert "directory listing denied" in caught.value.reason


def test_scan_missing_directory_is_complete_empty_history(task_dir: Path) -> None:
    missing = task_dir.parent / "missing-tasks"

    scan = store.scan(missing)

    assert scan.records == []
    assert scan.refusals == {}
    assert not scan.history_incomplete
    assert store.read_all(missing) == []

