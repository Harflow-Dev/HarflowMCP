"""Public continuation requests preserve text and reject invalid budgets."""
from types import SimpleNamespace

import pytest
from mcp import MCPError

from harflow_mcp import ctl, id_resolution, server, state_ownership
from harflow_mcp.state_layout import resolve_layout


@pytest.mark.parametrize("budget", [True, -1, 11, 1.5, "1"])
async def test_invalid_auto_budget_is_rejected_before_dispatch(budget):
    with pytest.raises(MCPError, match="auto_continue"):
        await server.start_task("request", "/tmp", auto_continue=budget)


async def test_workflow_cannot_enable_standalone_auto_continuation():
    with pytest.raises(MCPError, match="Workflow retries"):
        await server.start_task("request", "/tmp", workflow="saved", auto_continue=1)


async def test_continue_tool_passes_exact_text_and_notes(monkeypatch):
    observed = []

    async def guard(task_id):
        observed.append(("guard", task_id))
        return id_resolution.Resolution(task_id, None)

    async def continue_task(task_id, extra_prompt, notes_path, *, consume_followups=False, background_idle_seconds=None):
        assert consume_followups is True  # a caller-initiated continuation consumes queued follow-ups
        assert background_idle_seconds is None
        observed.append((task_id, extra_prompt, notes_path))
        return SimpleNamespace(brief=lambda: {"task_id": "new"}, enforcement={})

    async def direct_caller():
        return None

    monkeypatch.setattr(server, "_guard_task_read", guard)
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(continue_task=continue_task, _mutation_caller=direct_caller))
    result = await server.continue_task("old", "  next\nstep  ", "/tmp/notes")
    assert result["task_id"] == "new"
    assert observed == [("guard", "old"), ("old", "  next\nstep  ", "/tmp/notes")]


def test_continue_cli_file_preserves_prose(tmp_path, monkeypatch):
    text = "  next\r\nstep  "
    path = tmp_path / "followup"
    path.write_bytes(text.encode("utf-8"))
    state_root = tmp_path / "engine-state"
    with state_ownership.state_session(resolve_layout(root=state_root)):
        pass
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(state_root))
    observed = []

    async def continue_task(task_id, extra_prompt, notes_path, *, consume_followups=False, background_idle_seconds=None):
        assert consume_followups is True
        assert background_idle_seconds is None
        observed.append((task_id, extra_prompt, notes_path))
        return "task"

    def detached(args, action, resolution=None):
        import asyncio
        assert asyncio.run(action(SimpleNamespace(continue_task=continue_task))) == "task"
        return 0

    monkeypatch.setattr(ctl, "_cmd_detached", detached)
    # The task is a stub; prefix resolution has its own tests.
    monkeypatch.setattr(id_resolution, "resolve_task", lambda log_dir, value, **kw: id_resolution.Resolution(value, None))
    assert ctl.main(["continue", "old", "--extra-prompt-file", str(path), "--notes-path", "/tmp/notes"]) == 0
    assert observed == [("old", text, "/tmp/notes")]
