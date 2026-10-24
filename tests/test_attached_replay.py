"""Incremental replay behavior for attached task snapshots."""

from __future__ import annotations

import json
import copy
from pathlib import Path

import pytest

from harflow_mcp import attached_replay, store
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session


def _line(result: str) -> bytes:
    return (json.dumps({"type": "result", "subtype": "success", "is_error": False, "result": result, "session_id": "s", "num_turns": 1, "total_cost_usd": 0, "usage": {"input_tokens": 1, "output_tokens": 1}, "permission_denials": []}, ensure_ascii=False) + "\n").encode()


def _init(root: Path) -> None:
    with state_session(resolve_layout(root=root)):
        pass


def _replay(root: Path, task_id: str = "a" * 32):
    with state_session(resolve_layout(root=root)):
        return attached_replay.replay(root / "tasks", task_id, "claude")


def test_incremental_replay_matches_store_and_does_not_reingest_unchanged_bytes(tmp_path, monkeypatch):
    _init(tmp_path)
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    path = tasks / ("a" * 32 + ".jsonl")
    path.write_bytes(_line("first"))
    calls = 0
    original = attached_replay._apply

    def counted(raw, state, tail, backend):
        nonlocal calls
        calls += 1
        return original(raw, state, tail, backend)

    monkeypatch.setattr(attached_replay, "_apply", counted)
    first = _replay(tmp_path)
    baseline = store.replay_log(tasks, "a" * 32, "claude")
    assert first[0].summary == baseline[0].summary and first[1] == baseline[1]
    assert calls == 1
    assert _replay(tmp_path)[0].summary == "first"
    assert calls == 1
    with path.open("ab") as handle:
        handle.write(_line("second"))
    assert _replay(tmp_path)[0].summary == "second"
    assert calls == 2


def test_partial_utf8_and_newline_are_committed_only_after_completion(tmp_path):
    _init(tmp_path)
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    path = tasks / ("a" * 32 + ".jsonl")
    raw = _line("résumé")
    split = raw.index(b"\xc3\xa9") + 1
    path.write_bytes(raw[:split])
    with pytest.raises(UnicodeDecodeError):
        store.replay_log(tasks, "a" * 32, "claude")
    with pytest.raises(UnicodeDecodeError):
        _replay(tmp_path)
    path.write_bytes(raw)
    cached = _replay(tmp_path)
    uncached = store.replay_log(tasks, "a" * 32, "claude")
    assert cached[0].summary == uncached[0].summary == "résumé"
    assert cached[1] == uncached[1] and len(cached[1]) == 1


@pytest.mark.parametrize("newline", [b"\n", b"\r", b"\r\n"])
def test_universal_newlines_and_final_unterminated_json_match_store(tmp_path, newline):
    _init(tmp_path)
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    path = tasks / ("a" * 32 + ".jsonl")
    first = _line("first").rstrip(b"\n") + newline
    path.write_bytes(first)
    cached = _replay(tmp_path)
    uncached = store.replay_log(tasks, "a" * 32, "claude")
    assert cached[0].summary == uncached[0].summary == "first"
    path.write_bytes(first + _line("second").rstrip(b"\n"))
    cached = _replay(tmp_path)
    uncached = store.replay_log(tasks, "a" * 32, "claude")
    assert cached[0].summary == uncached[0].summary == "second"
    assert cached[1] == uncached[1]


def test_unicode_line_separators_are_literal_json_content(tmp_path):
    _init(tmp_path)
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    path = tasks / ("a" * 32 + ".jsonl")
    path.write_bytes(_line("literal \u0085 and \u2028"))
    cached = _replay(tmp_path)
    uncached = store.replay_log(tasks, "a" * 32, "claude")
    assert cached[0].summary == uncached[0].summary == "literal \u0085 and \u2028"
    assert cached[1] == uncached[1]


def test_cache_isolated_from_caller_mutation_and_resets_on_inode_truncate_and_backend_replace(tmp_path, monkeypatch):
    _init(tmp_path)
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    task_id = "a" * 32
    path = tasks / f"{task_id}.jsonl"
    path.write_bytes(_line("done"))
    state, tail = _replay(tmp_path)
    state.summary = "caller mutation"
    tail.append("caller mutation")
    fresh, fresh_tail = _replay(tmp_path)
    assert fresh.summary == "done" and fresh_tail != tail

    path.write_bytes(_line("new"))
    reset, _ = _replay(tmp_path)
    assert reset.summary == "new"

    replacement = tmp_path / "replacement.jsonl"
    replacement.write_bytes(_line("replacement"))
    replacement.replace(path)
    replaced, _ = _replay(tmp_path)
    assert replaced.summary == "replacement"

    original_get = attached_replay.get_backend
    replacement_backend = copy.copy(original_get("claude"))
    monkeypatch.setattr(attached_replay, "get_backend", lambda _name: replacement_backend)
    # A replacement backend identity starts a fresh isolated cache entry and
    # cannot expose the old accumulator.
    fresh_backend, _ = _replay(tmp_path)
    assert fresh_backend.summary == "replacement"
    monkeypatch.setattr(attached_replay, "get_backend", original_get)


def test_missing_or_unreadable_log_never_returns_stale_completion(tmp_path):
    _init(tmp_path)
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    task_id = "a" * 32
    path = tasks / f"{task_id}.jsonl"
    path.write_bytes(_line("complete"))
    assert _replay(tmp_path)[0].summary == "complete"
    path.unlink()
    assert _replay(tmp_path)[0].summary is None
    path.mkdir()
    assert _replay(tmp_path)[0].summary is None


def test_replay_cache_is_bounded_to_thirty_two_entries(tmp_path):
    _init(tmp_path)
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    for index in range(33):
        task_id = f"{index:032x}"
        (tasks / f"{task_id}.jsonl").write_bytes(_line(str(index)))
        _replay(tmp_path, task_id)
    assert len(attached_replay._entries) <= 32
