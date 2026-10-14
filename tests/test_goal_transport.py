"""Goal launch/revision controls preserve caller text across CLI and MCP boundaries."""
import pytest

from polybridge import ctl, server


@pytest.mark.parametrize("monitor", [False, True])
@pytest.mark.parametrize("command", ["workflow-start", "workflow-revise-goal", "run"])
def test_goal_files_preserve_multiline_text(tmp_path, monkeypatch, monitor, command):
    text = "  Compare these screens\r\nagainst the supplied reference.\n  "
    path = tmp_path / "goal.txt"
    path.write_bytes(text.encode("utf-8"))
    observed = {}

    async def call(action, **kwargs):
        observed.update(action=action, **kwargs)
        return {"workflow_run_id": "r1"}

    monkeypatch.setattr(server, "_workflow_call", call)
    if command == "workflow-revise-goal":
        args = [command, "r1", "--expected-revision", "2"]
    elif command == "run":
        args = [command, "--workflow", "saved", "--repo", str(tmp_path), "--prompt", "request"]
    else:
        args = [command, "saved", "--repo", str(tmp_path), "--prompt", "request"]
    args += ["--goal-file", str(path), "--json"]
    if monitor:
        args += ["--monitor"]
    assert ctl.main(args) == 0
    assert observed["goal"] == text
    assert observed.get("interaction_owner") == ("monitor" if monitor else None)
    assert observed["action"] == ("revise_goal" if command == "workflow-revise-goal" else "start")
    if command == "workflow-revise-goal":
        assert observed["expected_revision"] == 2


def test_missing_goal_file_never_dispatches(tmp_path, monkeypatch):
    async def call(*args, **kwargs):
        pytest.fail("Missing goal must not dispatch")

    monkeypatch.setattr(server, "_workflow_call", call)
    assert ctl.main(["workflow-start", "saved", "--repo", str(tmp_path), "--prompt", "request",
                     "--goal-file", str(tmp_path / "absent"), "--json"]) == 1


async def test_mcp_goal_launch_and_revision_pass_exact_text(monkeypatch):
    observed = []

    async def call(action, **kwargs):
        observed.append((action, kwargs))
        return {"workflow_run_id": "r1", "status": "paused"}

    monkeypatch.setattr(server, "_workflow_call", call)
    goal = "  line one\nline two  "
    await server.start_workflow("saved", "request", "/tmp", goal=goal)
    await server.revise_workflow_goal("r1", goal, 1)
    assert observed[0][1]["goal"] == goal
    assert observed[1] == ("revise_goal", {"run_id": "r1", "goal": goal, "expected_revision": 1})
