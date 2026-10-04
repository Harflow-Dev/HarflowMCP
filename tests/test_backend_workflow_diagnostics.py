"""Backend-owned workflow evidence never trusts assistant/tool prose."""
import json

from polybridge import workflows as w
from polybridge.backends.claude import ALLOWED_TOOLS
from polybridge.backends.vibe import VibeBackend


def test_publish_adds_narrow_review_comment_commands_only():
    rules = ALLOWED_TOOLS["publish"].split(",")
    assert rules == ["Bash(git commit:*)", "Bash(git push:*)"]
    assert "Bash(gh pr review:*)" not in rules
    assert "Bash(gh pr comment:*)" not in rules
    for blocked in ("Bash(gh api:*)", "Bash(gh pr merge:*)", "Bash(gh pr close:*)", "Bash(gh repo edit:*)", "Bash(gh repo delete:*)"):
        assert blocked not in rules


def test_vibe_native_session_config_identity_has_provenance(tmp_path, monkeypatch):
    monkeypatch.setenv("VIBE_HOME", str(tmp_path))
    session_id = "12345678-full-native-session-id"
    folder = tmp_path / "logs" / "session" / "session_20261004_12345678"
    folder.mkdir(parents=True)
    (folder / "meta.json").write_text(json.dumps({"session_id": session_id, "config": {"active_model": "glm", "models": [{"alias": "glm", "name": "GLM-5.3", "thinking": "low", "provider": "provider"}]}}))
    metadata = w.observed_harness_metadata({"backend": "vibe", "session_id": session_id, "summary": "I am another model"}, {"requested": {"backend": "vibe"}})
    assert metadata["observed"] == {"active_model": "glm", "model": "GLM-5.3", "reasoning_effort": "low", "provider": "provider"}
    assert metadata["verification_status"] == "observed_configuration"
    assert metadata["provenance"] == "harness_session_configuration"
    assert VibeBackend.workflow_observed_metadata({"session_id": "12345678-wrong-session"}) is None


def test_vibe_missing_metadata_does_not_invent_model(tmp_path, monkeypatch):
    monkeypatch.setenv("VIBE_HOME", str(tmp_path))
    assert VibeBackend.workflow_observed_metadata({"session_id": "missing", "summary": "active_model=glm"}) is None


def test_backend_seam_controls_availability_and_diagnostics(tmp_path, monkeypatch):
    class Adapter:
        @staticmethod
        def workflow_stderr_availability_failure(text):
            return "adapter outage" if text == "native stderr" else None
        @staticmethod
        def workflow_availability_failure(event):
            return "adapter envelope outage" if event.get("native") == "error" else None
        @staticmethod
        def workflow_failure_diagnostic(event):
            return "adapter startup diagnostic" if event.get("native") == "error" else None
    monkeypatch.setitem(w.backends.BACKENDS, "future-harness", Adapter())
    path = tmp_path / "native.jsonl"
    path.write_text(json.dumps({"native": "error"}) + "\n")
    assert w.availability_failure({"status": "failed", "backend": "future-harness", "stderr_tail": ["native stderr"]}) == "adapter outage"
    snapshot = {"status": "failed", "backend": "future-harness", "raw_stream_log": str(path)}
    assert w.availability_failure(snapshot) == "adapter envelope outage"
    assert w.failure_diagnostic(snapshot, "request") == "adapter startup diagnostic"


def test_codex_rate_limit_status_preserves_quota_fallback():
    from polybridge.backends.codex import CodexBackend
    event = {"type": "turn.failed", "error": {"code": "rate_limit_exceeded", "status": 429}}
    assert CodexBackend.workflow_availability_failure(event) == "codex availability rejected"
    for status in (400, 401, 403):
        event["error"]["status"] = status
        assert CodexBackend.workflow_availability_failure(event) is None
    event["error"] = {"status": 429, "message": "rate_limit_exceeded"}
    assert CodexBackend.workflow_availability_failure(event) is None


def test_failed_stream_diagnostics_use_bounded_shared_tail(tmp_path, monkeypatch):
    from pathlib import Path
    from contextlib import contextmanager
    path = tmp_path / "large.jsonl"
    path.write_bytes((b'{"type":"assistant","text":"' + b'x' * 1000 + b'"}\n') * 4000 + b'{"native":"error"}\n')
    class Adapter:
        workflow_stderr_availability_failure = staticmethod(lambda text: None)
        workflow_availability_failure = staticmethod(lambda event: "late quota" if event.get("native") == "error" else None)
        workflow_failure_diagnostic = staticmethod(lambda event: "late diagnostic" if event.get("native") == "error" else None)
    monkeypatch.setitem(w.backends.BACKENDS, "bounded-harness", Adapter())
    original = Path.open
    reads = []
    @contextmanager
    def tracked_open(self, *args, **kwargs):
        with original(self, *args, **kwargs) as file:
            class Reader:
                def read(self, size=-1):
                    assert 0 <= size <= 256 * 1024
                    value = file.read(size)
                    reads.append(len(value))
                    return value
                def seek(self, *args):
                    return file.seek(*args)
            yield Reader()
    monkeypatch.setattr(Path, "open", tracked_open)
    snapshot = {"status": "failed", "backend": "bounded-harness", "raw_stream_log": str(path)}
    assert w.availability_failure(snapshot) == "late quota"
    assert w.failure_diagnostic(snapshot, "Request") == "late diagnostic"
    assert sum(reads) <= 512 * 1024
    assert len(reads) == 2


def test_bounded_stream_cache_refreshes_after_log_growth(tmp_path):
    from polybridge.backends.workflow_diagnostics import stream_events
    path = tmp_path / "growing.jsonl"
    path.write_text('{"type":"assistant"}\n')
    assert stream_events(path) == ({"type": "assistant"},)
    with path.open("a") as stream:
        stream.write('{"type":"turn.failed","error":{"code":"rate_limit_exceeded"}}\n')
    assert stream_events(path)[-1]["type"] == "turn.failed"
    assert w.availability_failure({"status": "failed", "backend": "codex", "raw_stream_log": str(path)}) == "codex availability rejected"


def test_bounded_stream_skips_oversized_entry_preserves_terminal_error(tmp_path):
    from polybridge.backends.workflow_diagnostics import stream_events, STREAM_WINDOW_EVENTS
    path = tmp_path / "oversized.jsonl"
    path.write_bytes(b'{"type":"assistant","text":"' + b'x' * (2 * 1024 * 1024) + b'"}\n' + b'{"type":"error","error":{"status":503}}\n')
    events = stream_events(path)
    assert len(events) <= 2 * STREAM_WINDOW_EVENTS
    assert events == ({"type": "error", "error": {"status": 503}},)


def test_small_dense_stream_keeps_terminal_error_after_event_cap(tmp_path):
    from polybridge.backends.workflow_diagnostics import stream_events, STREAM_WINDOW_EVENTS, STREAM_WINDOW_BYTES
    path = tmp_path / "dense.jsonl"
    path.write_bytes(b'{"type":"assistant"}\n' * 700 + b'{"type":"turn.failed","error":{"code":"rate_limit_exceeded"}}\n')
    assert path.stat().st_size < STREAM_WINDOW_BYTES
    events = stream_events(path)
    assert len(events) == 2 * STREAM_WINDOW_EVENTS
    assert events[-1]["type"] == "turn.failed"
    assert w.availability_failure({"status": "failed", "backend": "codex", "raw_stream_log": str(path)}) == "codex availability rejected"


def test_small_stream_event_windows_do_not_duplicate_overlap(tmp_path):
    from polybridge.backends.workflow_diagnostics import stream_events
    path = tmp_path / "overlap.jsonl"
    path.write_text("".join(json.dumps({"index": index}) + "\n" for index in range(300)))
    assert [event["index"] for event in stream_events(path)] == list(range(300))
