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
