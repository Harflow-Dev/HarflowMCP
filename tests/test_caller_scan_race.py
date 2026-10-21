"""Bounded caller-candidate retry for a task document replaced during a read."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from harflow_mcp import lineage, state_ownership, store, task_documents
from harflow_mcp.lineage import detect_caller as real_detect_caller
from harflow_mcp.lineage import detect_caller_detail as real_detect_caller_detail
from harflow_mcp.state_layout import resolve_layout


def _record(task_id: str = "caller", **overrides: object) -> store.TaskRecord:
    now = datetime.now(timezone.utc).isoformat()
    values: dict[str, object] = {
        "task_id": task_id,
        "backend": "claude",
        "session_id": "session-1",
        "markers": ["session-1"],
        "repo_path": "/tmp/repo",
        "started_at": now,
        "status": "running",
        "exit_code": None,
        "pid": 4242,
        "pgid": 3131,
        "start_time": "start-1",
    }
    values.update(overrides)
    return store.TaskRecord(**values)


def _refusal(
    task_id: str,
    *,
    changed: object = False,
    code: str = "task_unavailable",
    reason: str = "refused",
):
    error = task_documents.TaskDocumentError(task_id, reason, code=code)
    if changed is not False:
        error._source_changed_during_read = changed
    return error


def _scan(*, records=(), refusals=()):
    return store.TaskScan(list(records), {key: error for key, error in refusals})


def test_candidate_scan_retries_one_atomic_replacement_and_uses_only_fresh_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    before = _record(title="before")
    after = replace(before, title="after")
    with state_ownership.state_session(resolve_layout(root=root)):
        store.write(log_dir, before)
        path = store.record_path(log_dir, before.task_id)
        real_stat = Path.stat
        raced = False

        def atomic_writer(candidate: Path, *args, **kwargs):
            nonlocal raced
            result = real_stat(candidate, *args, **kwargs)
            if candidate == path and not raced:
                raced = True
                store.write(log_dir, after)
            return result

        monkeypatch.setattr(Path, "stat", atomic_writer)
        candidates = lineage.read_candidates(log_dir).resolve()

    assert raced
    assert [record.title for record in candidates] == ["after"]


def test_direct_read_still_refuses_the_same_replacement_race(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    before = _record(title="before")
    after = replace(before, title="after")
    with state_ownership.state_session(resolve_layout(root=root)):
        store.write(log_dir, before)
        path = store.record_path(log_dir, before.task_id)
        real_stat = Path.stat
        raced = False

        def atomic_writer(candidate: Path, *args, **kwargs):
            nonlocal raced
            result = real_stat(candidate, *args, **kwargs)
            if candidate == path and not raced:
                raced = True
                store.write(log_dir, after)
            return result

        monkeypatch.setattr(Path, "stat", atomic_writer)
        with pytest.raises(store.TaskDocumentError) as caught:
            store.read(log_dir, before.task_id, include_prompt=False)

    assert caught.value.code == "task_unavailable"
    assert getattr(caught.value, "_source_changed_during_read") is True


def test_ordinary_candidate_scan_success_and_failure_are_memoized(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    calls = []
    successful = _scan(records=[_record()])
    monkeypatch.setattr(store, "scan", lambda _: calls.append(1) or successful)
    with state_ownership.state_session(resolve_layout(root=root)):
        candidate_scan = lineage.read_candidates(log_dir)
        cached = candidate_scan.resolve()
        assert [record.task_id for record in cached] == ["caller"]
        assert real_detect_caller(
            log_dir,
            environ={},
            getsid=lambda _: 555,
            getpid=lambda: 999,
            process_table=lambda: {999: 1},
            check=lambda _: "dead",
            candidates=candidate_scan,
        ) is None
        assert real_detect_caller_detail(
            log_dir,
            environ={},
            getsid=lambda _: 555,
            getpid=lambda: 999,
            process_table=lambda: {999: 1},
            check=lambda _: "dead",
            candidates=candidate_scan,
        ) == lineage.Detection(None)
        assert len(calls) == 1

    failure = _scan(refusals=[("bad", _refusal("bad", code="task_incompatible"))])
    calls.clear()
    monkeypatch.setattr(store, "scan", lambda _: calls.append(1) or failure)
    with state_ownership.state_session(resolve_layout(root=root)):
        scan = lineage.read_candidates(log_dir)
        first = scan.resolve()
        second = scan.resolve()
        assert real_detect_caller_detail(
            log_dir,
            environ={},
            getsid=lambda _: 555,
            getpid=lambda: 999,
            process_table=lambda: {999: 1},
            check=lambda _: "dead",
            candidates=scan,
        ).undecidable is not None
    assert isinstance(first, lineage._CandidateReadFailure)
    assert first is second
    assert len(calls) == 1


@pytest.mark.parametrize(
    "refusals",
    [
        [("changed", _refusal("changed", changed=True)), ("bad", _refusal("bad", code="task_incompatible"))],
        [("bad", _refusal("bad", code="task_incompatible")), ("changed", _refusal("changed", changed=True))],
        [("changed", _refusal("changed", changed=True)), ("io", _refusal("io", code="task_unavailable"))],
        [("io", _refusal("io", code="task_unavailable")), ("changed", _refusal("changed", changed=True))],
        [(
            "ordinary",
            _refusal(
                "ordinary",
                code="task_unavailable",
                reason="Task metadata changed while reading",
            ),
        )],
        [("invalid/id", _refusal("invalid/id", code="task_incompatible")), ("changed", _refusal("changed", changed=True))],
    ],
)
def test_mixed_or_unmarked_refusals_never_retry(monkeypatch, tmp_path: Path, refusals) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    calls = []
    scan = _scan(refusals=refusals)
    monkeypatch.setattr(store, "scan", lambda _: calls.append(1) or scan)
    with state_ownership.state_session(resolve_layout(root=root)):
        result = lineage.read_candidates(log_dir).resolve()
    assert isinstance(result, lineage._CandidateReadFailure)
    assert len(calls) == 1


def test_truthy_but_non_boolean_marker_never_retries(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    calls = []
    scan = _scan(refusals=[("truthy", _refusal("truthy", changed=1))])
    monkeypatch.setattr(store, "scan", lambda _: calls.append(1) or scan)
    with state_ownership.state_session(resolve_layout(root=root)):
        result = lineage.read_candidates(log_dir).resolve()
    assert isinstance(result, lineage._CandidateReadFailure)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "second_error",
    [
        _refusal("churn", code="task_unavailable"),
        _refusal("churn", code="task_incompatible"),
        _refusal("churn", changed=True, code="task_unavailable"),
    ],
    ids=["unavailable", "incompatible", "source-changed"],
)
def test_persistent_source_churn_is_fail_closed_after_two_scans(
    monkeypatch, tmp_path: Path, second_error: task_documents.TaskDocumentError
) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    calls = []
    first = _scan(refusals=[("churn", _refusal("churn", changed=True))])
    second = _scan(refusals=[("churn", second_error)])
    monkeypatch.setattr(store, "scan", lambda _: calls.append(1) or (first if len(calls) == 1 else second))
    with state_ownership.state_session(resolve_layout(root=root)):
        candidate_scan = lineage.read_candidates(log_dir)
        result = candidate_scan.resolve()
        assert isinstance(result, lineage._CandidateReadFailure)
        assert real_detect_caller(
            log_dir, environ={}, getsid=lambda _: 555, getpid=lambda: 999,
            process_table=lambda: {999: 1}, check=lambda _: "dead", candidates=candidate_scan
        ) is None
        detail = real_detect_caller_detail(
            log_dir, environ={}, getsid=lambda _: 555, getpid=lambda: 999,
            process_table=lambda: {999: 1}, check=lambda _: "dead", candidates=candidate_scan
        )
    assert isinstance(result, lineage._CandidateReadFailure)
    assert result.exc is second_error
    assert detail.undecidable is not None
    assert str(second_error) in detail.undecidable
    assert len(calls) == 2


def test_partial_first_scan_is_discarded_when_fresh_scan_succeeds(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    stale = _record("stale", title="stale")
    fresh = _record("fresh", title="fresh", pgid=555)
    calls = []
    first = _scan(records=[stale], refusals=[("changed", _refusal("changed", changed=True))])
    second = _scan(records=[fresh])
    monkeypatch.setattr(store, "scan", lambda _: calls.append(1) or (first if len(calls) == 1 else second))
    with state_ownership.state_session(resolve_layout(root=root)):
        candidate_scan = lineage.read_candidates(log_dir)
        caller = real_detect_caller(
            log_dir,
            environ={lineage.ENV_TASK_ID: "fresh"},
            getsid=lambda _: 555,
            getpid=lambda: 999,
            process_table=lambda: {999: 1},
            check=lambda _: "alive",
            candidates=candidate_scan,
        )
        detail = real_detect_caller_detail(
            log_dir,
            environ={lineage.ENV_TASK_ID: "fresh"},
            getsid=lambda _: 555,
            getpid=lambda: 999,
            process_table=lambda: {999: 1},
            check=lambda _: "alive",
            candidates=candidate_scan,
        )
    assert caller is not None and caller.record.task_id == "fresh"
    assert detail.caller is not None and detail.caller.record.task_id == "fresh"
    assert [record.task_id for record in candidate_scan.resolve()] == ["fresh"]
    assert detail.caller.record.task_id != stale.task_id
    assert len(calls) == 2


def test_marker_is_process_local_and_never_serialized() -> None:
    error = _refusal("changed", changed=True)
    assert "source_changed" not in json.dumps(error.as_dict())


def test_payload_marker_cannot_authorize_a_retry(tmp_path: Path) -> None:
    root = tmp_path / "state"
    log_dir = root / "tasks"
    path = store.record_path(log_dir, "foreign")
    with state_ownership.state_session(resolve_layout(root=root)):
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({
            "product": "other-product",
            "format": "other.task",
            "v": 1,
            "_source_changed_during_read": True,
        }), encoding="utf-8")
        with pytest.raises(store.TaskDocumentError) as caught:
            store.read(log_dir, "foreign", include_prompt=False)
    assert getattr(caught.value, "_source_changed_during_read", False) is not True
