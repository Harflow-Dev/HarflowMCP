"""`harflow-ctl task-prompt`: bounded prompt text with integrity and availability states."""
from __future__ import annotations

import hashlib
import json
import os

import pytest

from conftest import ALIVE_OWNER

from harflow_mcp import ctl, state_ownership, store, takeover, task_prompt_read
from harflow_mcp.state_layout import resolve_layout

TASK = "task-prompt-1"


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield layout


def seed(state, prompt, *, task_id=TASK, title="Fixture title"):
    record = store.TaskRecord(task_id=task_id, backend="claude", session_id="s1", markers=["s1"], repo_path="/repo",
                              started_at="2027-01-01T00:00:00+00:00", status="completed", exit_code=0,
                              finished_at="2027-01-01T00:00:10+00:00", owner=ALIVE_OWNER, prompt=prompt, title=title)
    store.write(state.tasks_directory, record)
    return record


def sidecar(state, task_id=TASK):
    paths = list(state.tasks_directory.glob(f"{task_id}.prompt.*.txt"))
    assert len(paths) == 1
    return paths[0]


def run(capsys, *args, expect=0, task=TASK):
    code = ctl.main(["task-prompt", task, *args, "--json"])
    captured = capsys.readouterr()
    assert code == expect, captured.err
    assert len(captured.out.splitlines()) == 1
    document = json.loads(captured.out)
    return document["result"] if expect == 0 else document["error"]


def test_inline_prompt_is_verbatim_and_complete(state, capsys):
    prompt = "  Keep\tthe   spaces\n\nand  newlines  "
    seed(state, prompt)
    result = run(capsys)
    assert result["task_id"] == TASK and result["title"] == "Fixture title"
    assert result["prompt"] == {"state": "available", "text": prompt, "returned_bytes": len(prompt), "total_bytes": len(prompt),
                                "truncated": False, "complete": True, "integrity": "not_applicable"}


def test_sidecar_prompt_is_verified_and_whole(state, capsys):
    prompt = "line one\n" * 600
    seed(state, prompt)
    assert store.read(state.tasks_directory, TASK, include_prompt=False).prompt_truncated
    result = run(capsys)["prompt"]
    assert result["state"] == "available" and result["text"] == prompt and result["integrity"] == "verified"
    assert result["total_bytes"] == result["returned_bytes"] == len(prompt.encode()) and result["complete"] is True


def test_max_bytes_cuts_on_a_character_boundary(state, capsys):
    prompt = "é" * 3000
    seed(state, prompt)
    result = run(capsys, "--max-bytes", "1001")["prompt"]
    assert result["truncated"] is True and result["complete"] is False
    assert result["text"] == "é" * 500 and result["returned_bytes"] == 1000 and result["total_bytes"] == 6000
    assert result["integrity"] == "verified"
    inline = seed(state, "é" * 10, task_id="inline-1")
    cut = run(capsys, "--max-bytes", "5", task="inline-1")["prompt"]
    assert cut["text"] == "éé" and cut["truncated"] is True and cut["total_bytes"] == 20 and inline


def test_missing_unreadable_and_special_sidecars_fall_back_to_the_preview(state, capsys, tmp_path):
    prompt = "p" * 5000
    seed(state, prompt)
    path = sidecar(state)
    expected = {"state": "preview_only", "text": prompt[:2000], "returned_bytes": 2000, "total_bytes": None,
                "truncated": False, "complete": False, "integrity": "not_applicable"}
    original = path.read_bytes()
    path.unlink()
    assert run(capsys)["prompt"] == expected
    target = tmp_path / "elsewhere.txt"
    target.write_bytes(original)
    path.symlink_to(target)
    assert run(capsys)["prompt"] == expected
    path.unlink()
    os.mkfifo(path)
    assert run(capsys)["prompt"] == expected
    path.unlink()
    path.mkdir()
    assert run(capsys)["prompt"] == expected


def test_digest_mismatch_reports_integrity_failed_without_text(state, capsys):
    seed(state, "q" * 5000)
    path = sidecar(state)
    path.write_bytes(b"q" * 4999 + b"r")
    result = run(capsys)["prompt"]
    assert result == {"state": "integrity_failed", "text": None, "returned_bytes": 0, "total_bytes": None,
                      "truncated": False, "complete": False, "integrity": "failed"}


def test_source_name_must_match_the_task_bound_pattern(state, capsys):
    seed(state, "s" * 5000)
    meta = state.tasks_directory / f"{TASK}.meta.json"
    document = json.loads(meta.read_text())
    document["prompt_source"] = "../outside.txt"
    meta.write_text(json.dumps(document))
    result = run(capsys)["prompt"]
    assert result["state"] == "unavailable" and result["text"] is None and result["complete"] is False
    document["prompt_source"] = f"other-task.prompt.{'0' * 64}.txt"
    meta.write_text(json.dumps(document))
    assert run(capsys)["prompt"]["state"] == "unavailable"


def test_large_sidecar_is_a_bounded_unverified_prefix(state, capsys, monkeypatch):
    seed(state, "seed " * 1000)
    old = sidecar(state)
    big = ("0123456789" * 1024 * 1024)[: 5 * 1024 * 1024]
    digest = hashlib.sha256(big.encode()).hexdigest()
    path = state.tasks_directory / f"{TASK}.prompt.{digest}.txt"
    path.write_text(big)
    meta = state.tasks_directory / f"{TASK}.meta.json"
    document = json.loads(meta.read_text())
    document["prompt_source"] = path.name
    meta.write_text(json.dumps(document))
    old.unlink()
    consumed = []
    real_read = os.read

    def counting(fd, length):
        data = real_read(fd, length)
        consumed.append(length)
        return data
    monkeypatch.setattr(task_prompt_read.os, "read", counting)
    result = run(capsys, "--max-bytes", "100")["prompt"]
    assert result["integrity"] == "unverified" and result["state"] == "available"
    assert result["text"] == big[:100] and result["truncated"] is True and result["complete"] is False
    assert result["total_bytes"] == len(big) and 104 in consumed and max(consumed) < 1_000_000


def test_prefix_ids_echo_resolved_from_and_usage_errors_precede(state, capsys):
    full = "0123456789abcdef0123456789abcdef"
    seed(state, "hello", task_id=full)
    assert run(capsys, task=full[:10])["resolved_from"]["task_id"] == full
    for arguments in (["--max-bytes", "0"], ["--max-bytes", "131073"]):
        with pytest.raises(SystemExit) as caught:
            ctl.main(["task-prompt", TASK, *arguments, "--json"])
        assert caught.value.code == 2
        assert json.loads(capsys.readouterr().out)["error"]["code"] == "usage"
    with pytest.raises(SystemExit):
        ctl.main(["task-prompt", TASK])
    capsys.readouterr()


def test_unknown_task_and_refused_callers(state, capsys, monkeypatch):
    seed(state, "hello")
    assert run(capsys, expect=1, task="task-does-not-exist")["code"] == "unknown_task"
    with monkeypatch.context() as refused:
        refused.setattr(takeover, "caller_refusal", lambda *_: ("agent_caller", "an agent"))
        error = run(capsys, expect=1, task="task-does-not-exist")
    assert error["code"] == "agent_caller"


def test_cut_drops_only_an_incomplete_trailing_character():
    cut = task_prompt_read._cut
    assert cut("aé".encode(), 2) == ("a", True) and cut("aé".encode(), 3) == ("aé", False)
    assert cut("a😀".encode(), 4) == ("a", True) and cut("a😀".encode(), 5) == ("a😀", False)
    assert cut(b"a\xffb" + b"c" * 5, 4) == ("a\ufffd", True)  # a, U+FFFD is 4 bytes
    assert cut(b"ab\xffcd", 6) == ("ab\ufffdc", True)
    assert cut(b"\xff\xff", 3) == ("\ufffd", True)
    for data in (b"a\xffb" + b"c" * 5, b"\xff" * 9, "é😀x".encode(), b"abc\xe2\x82"):
        for limit in range(1, 14):
            text, _ = cut(data, limit)
            assert len(text.encode()) <= limit
