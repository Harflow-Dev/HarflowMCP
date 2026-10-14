"""Public continuation requests preserve text and reject invalid budgets."""
from types import SimpleNamespace

import pytest
from mcp import MCPError

from polybridge import ctl, server


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

    async def continue_task(task_id, extra_prompt, notes_path):
        observed.append((task_id, extra_prompt, notes_path))
        return SimpleNamespace(brief=lambda: {"task_id": "new"}, enforcement={})

    monkeypatch.setattr(server, "_guard_task_read", guard)
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(continue_task=continue_task))
    result = await server.continue_task("old", "  next\nstep  ", "/tmp/notes")
    assert result["task_id"] == "new"
    assert observed == [("guard", "old"), ("old", "  next\nstep  ", "/tmp/notes")]


def test_continue_cli_file_preserves_prose(tmp_path, monkeypatch):
    text = "  next\r\nstep  "
    path = tmp_path / "followup"
    path.write_bytes(text.encode("utf-8"))
    observed = []

    async def continue_task(task_id, extra_prompt, notes_path):
        observed.append((task_id, extra_prompt, notes_path))
        return "task"

    def detached(args, action):
        import asyncio
        assert asyncio.run(action(SimpleNamespace(continue_task=continue_task))) == "task"
        return 0

    monkeypatch.setattr(ctl, "_cmd_detached", detached)
    assert ctl.main(["continue", "old", "--extra-prompt-file", str(path), "--notes-path", "/tmp/notes"]) == 0
    assert observed == [("old", text, "/tmp/notes")]
