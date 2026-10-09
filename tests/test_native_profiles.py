"""Access boundaries must survive direct native invocation construction."""
from types import SimpleNamespace

import pytest

from polybridge import backends
from polybridge.backends.claude import ClaudeBackend, UnsafeInvocationError as ClaudeUnsafe
from polybridge.backends.claude_native import ClaudeNativeAdapter
from polybridge.backends.codex import CodexBackend, UnsafeInvocationError as CodexUnsafe
from polybridge.backends.codex_native import CodexNativeAdapter


@pytest.mark.parametrize("backend,adapter,error", [(ClaudeBackend(), ClaudeNativeAdapter(), ClaudeUnsafe), (CodexBackend(), CodexNativeAdapter(), CodexUnsafe)])
def test_native_child_write_cannot_be_smuggled_through_read_only_owner(tmp_path, backend, adapter, error):
    model = "claude-sonnet-4-6" if isinstance(backend, ClaudeBackend) else "gpt-6.1-sol"
    invocation = adapter.configure(backend.build_start_argv("assignment", repo=tmp_path, freedom="read_only", session_id="00000000-0000-0000-0000-000000000001" if isinstance(backend, ClaudeBackend) else None, model=model, max_turns=100 if isinstance(backend, ClaudeBackend) else None, reasoning_effort=None), {"freedom": "write_in_repo"})
    with pytest.raises(error):
        backend.assert_safe(invocation, "read_only")


def test_codex_narrowing_requires_headless_because_cli_inherits_owner_sandbox(monkeypatch):
    monkeypatch.setattr(backends, "version", lambda _: "codex-cli 0.162.0")
    parent = SimpleNamespace(backend="codex", freedom="write_in_repo", network=False, model="gpt-6.1-sol", reasoning_effort=None, max_turns=None)
    reason = CodexNativeAdapter().eligible(parent, {"model": parent.model}, {"freedom": "read_only", "network": False})
    assert "inherits" in reason


@pytest.mark.parametrize("adapter,backend", [(ClaudeNativeAdapter(), "claude"), (CodexNativeAdapter(), "codex")])
def test_cached_unavailable_version_is_not_remeasured_but_runtime_is_fresh(monkeypatch, adapter, backend):
    calls = []
    monkeypatch.setattr(backends, "version", lambda harness: calls.append(harness) or None)
    parent = SimpleNamespace(backend=backend)
    assert adapter.eligible(parent, {}, {"backend_version": None})
    assert calls == []
    assert adapter.eligible(parent, {}, {})
    assert adapter.eligible(parent, {}, {})
    assert len(calls) == 2
