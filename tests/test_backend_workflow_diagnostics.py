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
