"""Process identity: telling a live task's process apart from a reused pid."""

from __future__ import annotations

import os
import subprocess

import pytest

from polybridge import identity


def test_capture_and_check_round_trip_for_our_own_process() -> None:
    captured = identity.capture(os.getpid(), [])

    assert captured is not None
    assert captured["pid"] == os.getpid()
    assert captured["start_time"]

    assert identity.identity_check(captured) == "alive"


def test_a_reused_pid_is_rejected_by_its_stale_start_time() -> None:
    """Our own pid is alive, but a *different* recorded start time means a different process."""
    captured = identity.capture(os.getpid(), [])
    assert captured is not None
    stale = {**captured, "start_time": "Wed Jan  1 00:00:00 2000"}

    assert identity.identity_check(stale) == "dead"


def test_ps_failure_is_undecidable(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args, **kwargs):
        raise OSError("ps unavailable")

    monkeypatch.setattr(identity.subprocess, "run", boom)

    captured = {"pid": os.getpid(), "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}
    assert identity.identity_check(captured) == "undecidable"
    assert identity.capture(os.getpid(), []) is None


def test_a_nonexistent_pid_is_dead() -> None:
    identity_dict = {"pid": 99999, "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}
    assert identity.identity_check(identity_dict) == "dead"


def test_capture_of_a_nonexistent_pid_returns_none() -> None:
    assert identity.capture(99999, []) is None


def test_legacy_record_with_matching_markers_is_undecidable() -> None:
    """No start_time on record — the weaker fallback test, reported conservatively."""
    proc = subprocess.run(["ps", "-o", "command=", "-p", str(os.getpid())], capture_output=True, text=True)
    own_command = proc.stdout.strip()
    marker = own_command.split()[0] if own_command else "python"

    legacy = {"pid": os.getpid(), "start_time": None, "markers": [marker]}
    assert identity.identity_check(legacy) == "undecidable"


def test_legacy_record_with_nonmatching_markers_is_dead() -> None:
    legacy = {"pid": os.getpid(), "start_time": None, "markers": ["definitely-not-in-the-cmdline"]}
    assert identity.identity_check(legacy) == "dead"


def test_legacy_record_with_no_markers_is_undecidable() -> None:
    legacy = {"pid": os.getpid(), "start_time": None, "markers": []}
    assert identity.identity_check(legacy) == "undecidable"


def test_legacy_record_of_a_dead_pid_is_dead_regardless_of_markers() -> None:
    legacy = {"pid": 99999, "start_time": None, "markers": ["anything"]}
    assert identity.identity_check(legacy) == "dead"


@pytest.mark.parametrize(
    "malformed",
    [None, {}, {"pid": "not-an-int"}, {"pid": -1}, {"pid": 0}, {"pid": True}, "not-a-mapping"],
)
def test_malformed_identity_is_undecidable(malformed) -> None:
    assert identity.identity_check(malformed) == "undecidable"


def test_a_matching_start_time_with_a_missing_marker_is_undecidable() -> None:
    captured = identity.capture(os.getpid(), [])
    assert captured is not None
    with_bad_marker = {**captured, "markers": ["definitely-not-in-the-cmdline"]}

    assert identity.identity_check(with_bad_marker) == "undecidable"


def test_own_identity_is_cached_and_alive() -> None:
    identity.own_identity.cache_clear()
    first = identity.own_identity()
    second = identity.own_identity()

    assert first is second
    assert first["pid"] == os.getpid()
    assert identity.identity_check(first) == "alive"
    identity.own_identity.cache_clear()


def test_own_identity_falls_back_when_capture_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    identity.own_identity.cache_clear()
    monkeypatch.setattr(identity, "capture", lambda pid, markers: None)

    result = identity.own_identity()

    assert result == {"pid": os.getpid(), "start_time": None, "markers": []}
    identity.own_identity.cache_clear()


# --- check_detail / may_signal / task_identity ------------------------------------------------


@pytest.mark.parametrize("malformed", [None, "not-a-mapping", {"pid": -1}, {"pid": True}])
def test_check_detail_reason_is_invalid_for_a_malformed_identity(malformed) -> None:
    assert identity.check_detail(malformed) == ("undecidable", "invalid")


def test_check_detail_reason_is_ps_failed_when_ps_cannot_run(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args, **kwargs):
        raise OSError("ps unavailable")

    monkeypatch.setattr(identity.subprocess, "run", boom)

    captured = {"pid": os.getpid(), "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}
    assert identity.check_detail(captured) == ("undecidable", "ps_failed")


def test_check_detail_reason_is_pid_absent_for_a_nonexistent_pid() -> None:
    identity_dict = {"pid": 99999, "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}
    verdict, reason = identity.check_detail(identity_dict)
    assert verdict == "dead"
    assert reason == "pid_absent"


def test_check_detail_reason_is_unparsable_for_garbled_ps_output(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = subprocess.CompletedProcess(args=[], returncode=0, stdout="not a ps line at all\n", stderr="")
    monkeypatch.setattr(identity, "_run_ps", lambda pid: fake)

    captured = {"pid": os.getpid(), "start_time": None, "markers": []}
    assert identity.check_detail(captured) == ("undecidable", "unparsable")


def test_check_detail_reason_is_start_time_differs_for_a_reused_pid() -> None:
    captured = identity.capture(os.getpid(), [])
    assert captured is not None
    stale = {**captured, "start_time": "Wed Jan  1 00:00:00 2000"}

    assert identity.check_detail(stale) == ("dead", "start_time_differs")


def test_check_detail_reason_is_start_time_match_for_our_own_process() -> None:
    captured = identity.capture(os.getpid(), [])
    assert captured is not None

    assert identity.check_detail(captured) == ("alive", "start_time_match")


def test_check_detail_reason_is_markers_missing_when_a_start_time_matches_but_a_marker_does_not() -> None:
    captured = identity.capture(os.getpid(), [])
    assert captured is not None
    with_bad_marker = {**captured, "markers": ["definitely-not-in-the-cmdline"]}

    assert identity.check_detail(with_bad_marker) == ("undecidable", "markers_missing")


def test_check_detail_reason_is_legacy_no_markers_for_a_legacy_record_without_markers() -> None:
    legacy = {"pid": os.getpid(), "start_time": None, "markers": []}
    assert identity.check_detail(legacy) == ("undecidable", "legacy_no_markers")


def test_check_detail_reason_is_legacy_markers_seen_when_they_match() -> None:
    proc = subprocess.run(["ps", "-o", "command=", "-p", str(os.getpid())], capture_output=True, text=True)
    own_command = proc.stdout.strip()
    marker = own_command.split()[0] if own_command else "python"

    legacy = {"pid": os.getpid(), "start_time": None, "markers": [marker]}
    assert identity.check_detail(legacy) == ("undecidable", "legacy_markers_seen")


def test_check_detail_reason_is_legacy_markers_not_seen_when_they_do_not_match() -> None:
    legacy = {"pid": os.getpid(), "start_time": None, "markers": ["definitely-not-in-the-cmdline"]}
    assert identity.check_detail(legacy) == ("dead", "legacy_markers_not_seen")


def test_may_signal_is_false_when_ps_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*args, **kwargs):
        raise OSError("ps unavailable")

    monkeypatch.setattr(identity.subprocess, "run", boom)

    captured = {"pid": os.getpid(), "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}
    assert identity.may_signal(captured) is False


def test_may_signal_is_true_for_legacy_markers_seen() -> None:
    proc = subprocess.run(["ps", "-o", "command=", "-p", str(os.getpid())], capture_output=True, text=True)
    own_command = proc.stdout.strip()
    marker = own_command.split()[0] if own_command else "python"

    legacy = {"pid": os.getpid(), "start_time": None, "markers": [marker]}
    assert identity.may_signal(legacy) is True


def test_may_signal_is_true_for_a_matching_start_time_and_markers() -> None:
    proc = subprocess.run(["ps", "-o", "command=", "-p", str(os.getpid())], capture_output=True, text=True)
    own_command = proc.stdout.strip()
    marker = own_command.split()[0] if own_command else "python"
    captured = identity.capture(os.getpid(), [marker])
    assert captured is not None

    assert identity.may_signal(captured) is True


def test_may_signal_is_false_when_a_start_time_matches_but_a_marker_is_missing() -> None:
    captured = identity.capture(os.getpid(), [])
    assert captured is not None
    with_bad_marker = {**captured, "markers": ["definitely-not-in-the-cmdline"]}

    assert identity.may_signal(with_bad_marker) is False


def test_may_signal_is_false_for_a_dead_process() -> None:
    identity_dict = {"pid": 99999, "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}
    assert identity.may_signal(identity_dict) is False


def test_task_identity_builds_the_expected_shape() -> None:
    built = identity.task_identity(123, "Wed Jan  1 00:00:00 2000", ["a", "b"])

    assert built == {"pid": 123, "start_time": "Wed Jan  1 00:00:00 2000", "markers": ["a", "b"]}
    assert identity.identity_check(identity.task_identity(99999, None, [])) == "dead"
