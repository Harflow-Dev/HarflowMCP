import json

import pytest

from polybridge import read_metrics


def test_disabled_by_default(monkeypatch, capsys):
    monkeypatch.delenv("PB_WORKFLOW_READ_METRICS", raising=False)
    with read_metrics.span("authority"):
        pass
    assert capsys.readouterr().err == ""


def test_enabled_fixed_content_free_schema(monkeypatch, capsys):
    monkeypatch.setenv("PB_WORKFLOW_READ_METRICS", "1")
    with read_metrics.span("catalog"):
        pass
    payload = json.loads(capsys.readouterr().err)
    assert set(payload) == {"workflow_read_metric_version", "stage", "duration_ms"}
    assert payload["stage"] == "catalog"
    assert payload["duration_ms"] >= 0


def test_exception_is_preserved(monkeypatch, capsys):
    monkeypatch.setenv("PB_WORKFLOW_READ_METRICS", "1")
    with pytest.raises(RuntimeError, match="original"):
        with read_metrics.span("projection"):
            raise RuntimeError("original")
    assert json.loads(capsys.readouterr().err)["stage"] == "projection"


def test_arbitrary_content_is_rejected(capsys):
    with pytest.raises(ValueError):
        with read_metrics.span("private-record-id"):
            pass
    assert capsys.readouterr().err == ""


def test_unavailable_diagnostic_stream_preserves_read(monkeypatch):
    monkeypatch.setenv("PB_WORKFLOW_READ_METRICS", "1")

    class ClosedStream:
        def write(self, value):
            raise OSError("closed")

    monkeypatch.setattr(read_metrics.sys, "stderr", ClosedStream())
    with read_metrics.span("serialization"):
        result = {"v": 1}
    assert result == {"v": 1}
